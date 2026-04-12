using System.Buffers;
using System.IO;
using System.Net;
using System.Net.Http;
using PCL.Core.IO.Net;
using PCL.Core.Logging;
using PCL.Core.Utils;
using PCL.Network.Loaders;
using PCL.Network.Scheduling;

namespace PCL.Network.Engine;

public class DownloadFile
{
    // This C# file owns the active downloader state machine, scheduler/finalization contracts, and future perf work.
    // Keep the legacy VB downloader only as behavioral reference when validating changes.
    private long _speedLastDone;
    private long _speedLastTime = TimeUtils.GetTimeTick();
    private long _cachedSpeed;
    private int _firstThreadSourceId;
    private bool _retried;
    private MemoryStream _smallFileCache;
    private int _singleSegmentRetryCount;
    private long _nextSingleSegmentRetryAllowedTime;

    private enum FileFinalizationAction
    {
        None,
        Merge,
        Retry,
        Fail
    }

    public int Id { get; } = ModBase.GetUuid();
    public string LocalPath { get; set; }
    public string LocalName => Path.GetFileName(LocalPath);
    public List<DownloadSource> AllSources { get; set; }
    public List<DownloadSource> OnceSources { get; } = new();
    public ModBase.FileChecker Check { get; set; }
    public bool UseBrowserUserAgent { get; set; }
    public string CustomUserAgent { get; set; }
    public bool AllowMultiThread { get; set; } = true;

    public NetState State { get; set; } = NetState.WaitingToCheck;
    public long TotalSize { get; set; } = -2;
    public bool IsUnknownSize { get; set; }
    public long DownloadedBytes { get; set; }
    public bool IsCopy { get; set; }
    public List<Exception> Errors { get; } = new();

    public DownloadSegment Segments { get; set; }

    public List<LoaderDownload> Loaders { get; } = new();

    public object LockState { get; } = new();
    public object LockChain { get; } = new();
    public object LockSource { get; } = new();
    public object LockDone { get; } = new();

    public int ConnectCount { get; set; }
    public long ConnectTime { get; set; }
    public object LockCount { get; } = new();

    public int AverageConnectTime
    {
        get
        {
            lock (LockCount)
            {
                return ConnectCount == 0 ? -1 : (int)(ConnectTime / (double)ConnectCount);
            }
        }
    }

    public long Speed
    {
        get
        {
            var now = TimeUtils.GetTimeTick();
            if (now - _speedLastTime > 200)
            {
                var delta = now - _speedLastTime;
                _cachedSpeed = (long)Math.Round((DownloadedBytes - _speedLastDone) / (delta / 1000.0));
                _speedLastDone = DownloadedBytes;
                _speedLastTime += delta;
            }
            return _cachedSpeed;
        }
    }

    public double Progress
    {
        get
        {
            return State switch
            {
                NetState.WaitingToCheck => 0,
                NetState.WaitingToDownload => 0.01,
                NetState.Connecting => 0.02,
                NetState.Reading => 0.04,
                NetState.Downloading => IsUnknownSize
                    ? 0.5
                    : 0.05 + 0.93 * (1 - Math.Pow(1 - DownloadedBytes / (double)Math.Max(TotalSize, 1), 0.9)),
                NetState.Merging => 0.99,
                NetState.Finished or NetState.Interrupted => 1,
                _ => 0.5
            };
        }
    }

    public bool IsNoSplit => IsUnknownSize || TotalSize < FilePieceLimit;

    public const long FilePieceLimit = 262144;
    public const long SmallFileMemoryLimit = 262144;

    private bool CanKeepSingleSegmentInMemory =>
        !IsUnknownSize && TotalSize >= 0 && TotalSize <= SmallFileMemoryLimit;

    private bool ShouldStartSingleSegmentInMemory =>
        IsNoSplit && (CanKeepSingleSegmentInMemory || IsUnknownSize);

    private bool IsSingleSegmentRetryCoolingDown =>
        _nextSingleSegmentRetryAllowedTime > TimeUtils.GetTimeTick();

    public DownloadFile(IEnumerable<string> urls, string localPath, ModBase.FileChecker checker = null,
        bool useBrowserUserAgent = false, string customUserAgent = "")
    {
        var distinctUrls = urls.Distinct().ToArray();
        AllSources = distinctUrls.Select((u, i) => new DownloadSource
        {
            Id = i,
            Url = ModSecret.SecretCdnSign(u.Replace("\r", "").Replace("\n", "").Trim()),
            FailCount = 0,
            IsFailed = false
        }).ToList();
        LocalPath = localPath;
        Check = checker;
        UseBrowserUserAgent = useBrowserUserAgent;
        CustomUserAgent = customUserAgent;
    }

    #region Source Management

    public bool HasAvailableSource(bool allowOnceSource = true)
    {
        lock (LockSource)
        {
            if (AllSources.Exists(s => !s.IsFailed)) return true;
            if (allowOnceSource && OnceSources.Count > 0) return true;
            return false;
        }
    }

    public DownloadSource GetAvailableSource(int startId = 0)
    {
        if (AllSources.Count == 0) return null;
        lock (LockSource)
        {
            if (HasAvailableSource(false))
            {
                for (int i = 0; i < AllSources.Count; i++)
                {
                    var idx = (startId + i) % AllSources.Count;
                    if (!AllSources[idx].IsFailed)
                        return AllSources[idx];
                }
            }
            return OnceSources.Count > 0 ? OnceSources[0] : null;
        }
    }

    #endregion

    #region TryBeginThread

    public DownloadSegment TryBeginThread()
    {
        return TryBeginThreadCore(runInline: false);
    }

    internal DownloadSegment TryBeginThreadInlineForTests()
    {
        return TryBeginThreadCore(runInline: true);
    }

    private DownloadSegment TryBeginThreadCore(bool runInline)
    {
        var reservedThreadSlot = false;
        try
        {
            if (!HasAvailableSource() ||
                State >= NetState.Merging || State == NetState.WaitingToCheck)
                return null;

            if (IsNoSplit && IsSingleSegmentRetryCoolingDown)
                return null;

            if (IsNoSplit && Segments != null &&
                Segments.State != NetState.Interrupted &&
                Segments.State != NetState.WaitingToDownload &&
                TimeUtils.GetTimeTick() - Segments.InitTime < 30000)
                return null;

            lock (LockState)
            {
                if (State < NetState.Connecting) State = NetState.Connecting;
            }

            long startPosition = 0;
            DownloadSource startSource = null;

            lock (LockChain)
            {
                if (IsNoSplit) goto Capture;
                if (!HasAvailableSource(false))
                {
                    if (OnceSources[0].SingleThreadId != null)
                    {
                        var existing = FindSegmentById(OnceSources[0].SingleThreadId.Value);
                        if (existing != null && existing.State != NetState.Interrupted) return null;
                    }
                }

            Capture:
                if (IsNoSplit && Segments != null &&
                    Segments.State != NetState.Interrupted && Segments.State != NetState.Finished)
                    return null;
                State = NetState.Reading;

                if (!TryResolveStartPointNoLock(out startPosition, out startSource, out var shouldResetState))
                    return null;

                if (shouldResetState)
                {
                    ResetTransferStateForFreshStart();
                    _firstThreadSourceId = startSource.Id + 1;
                }

            StartThread:
                if ((startPosition > TotalSize && TotalSize >= 0 && !IsUnknownSize) ||
                    startPosition < 0 || startSource == null)
                    return null;

                if (Loaders.Count == 0) return null;

                var segmentId = ModBase.GetUuid();
                var segmentInfo = new DownloadSegment
                {
                    Id = segmentId,
                    StartPosition = startPosition,
                    Source = startSource,
                    State = NetState.WaitingToDownload,
                    ParentFile = this
                };

                if (!DownloadSchedulerPolicy.TryReserveThreadSlot())
                    return null;

                reservedThreadSlot = true;

                if (segmentInfo.IsFirstSegment || Segments == null)
                {
                    Segments = segmentInfo;
                }
                else
                {
                    var currentChain = Segments;
                    while (currentChain.EndPosition <= startPosition)
                        currentChain = currentChain.Next;
                    segmentInfo.Next = currentChain.Next;
                    currentChain.Next = segmentInfo;
                }

                // Must set segmentInfo into thread before starting
                // thread captures segmentInfo via closure above
                lock (LockSource)
                {
                    if (!HasAvailableSource(false) && OnceSources.Count > 0)
                        OnceSources[0].SingleThreadId = segmentInfo.Id;
                }

                if (runInline)
                {
                    DownloadThread(segmentInfo);
                }
                else
                {
                    var thread = new Thread(() => DownloadThread(segmentInfo))
                    {
                        Name = $"NetTask {Loaders[0].Uuid}/{Id} Download {segmentId}#",
                        Priority = ThreadPriority.BelowNormal
                    };
                    thread.Start();
                }
                return segmentInfo;
            }
        }
        catch (Exception ex)
        {
            if (reservedThreadSlot)
                DownloadSchedulerPolicy.ReleaseThreadSlot();
            LogWrapper.Warn(ex, $"[Download] 尝试开始下载线程失败（{LocalName ?? "Nothing"}）");
            return null;
        }
    }

    private DownloadSegment FindSegmentById(int id)
    {
        for (var cur = Segments; cur != null; cur = cur.Next)
        {
            if (cur.Id == id) return cur;
        }
        return null;
    }

    private bool TryResolveStartPointNoLock(out long startPosition, out DownloadSource startSource, out bool shouldResetState)
    {
        startPosition = 0;
        startSource = null;
        shouldResetState = false;

        if (Segments == null)
        {
            shouldResetState = true;
            startSource = GetAvailableSource(_firstThreadSourceId);
            return startSource != null;
        }

        for (var cur = Segments; cur != null; cur = cur.Next)
        {
            if (cur.State == NetState.Interrupted && cur.RemainingBytes > 0)
            {
                startPosition = cur.StartPosition + cur.DownloadedBytes;
                startSource = GetAvailableSource(cur.Source.Id + 1);
                return startSource != null;
            }
        }

        var targetSource = GetAvailableSource();
        if (targetSource == null) return false;

        var targetUrl = targetSource.Url;
        if (!AllowMultiThread ||
            targetUrl.Contains("pcl2-server") || targetUrl.Contains("bmclapi") ||
            targetUrl.Contains("github.com") || targetUrl.Contains("optifine.net") ||
            targetUrl.Contains("modrinth") || targetUrl.Contains("gitcode") ||
            targetUrl.Contains("pysio.online") || targetUrl.Contains("mirrorchyan.com") ||
            targetUrl.Contains("naids.com"))
            return false;

        var maxRemaining = Segments;
        for (var cur = Segments; cur != null; cur = cur.Next)
        {
            if (cur.RemainingBytes > maxRemaining.RemainingBytes)
                maxRemaining = cur;
        }
        if (maxRemaining == null || maxRemaining.RemainingBytes < FilePieceLimit)
            return false;

        startPosition = maxRemaining.EndPosition - (long)(maxRemaining.RemainingBytes * 0.4);
        startSource = targetSource;
        return true;
    }

    private void ResetTransferStateForFreshStart()
    {
        _smallFileCache?.Dispose();
        _smallFileCache = null;
        Segments = null;

        lock (LockDone)
        {
            ModNet.NetManager.DownloadDone -= DownloadedBytes;
            DownloadedBytes = 0;
        }

        _speedLastDone = 0;
    }

    #endregion

    #region Download Thread

    private void DownloadThread(DownloadSegment seg)
    {
        if (ModBase.ModeDebug || seg.StartPosition == 0)
            ModBase.Log($"[Download] {LocalName} {seg.Id}#：开始，起始点 {seg.StartPosition}，{seg.Source.Url}");

        Stream resultStream = null;
        var timeout = GetTimeoutMilliseconds(seg);
        long contentLength = 0;
        seg.State = NetState.Connecting;
        bool interrupted = false;
        bool sourceBreak = false;
        bool notSupportRange = false;
        int httpDataCount = 0;

        try
        {
            if (OnceSources.Contains(seg.Source) && seg.Id != seg.Source.SingleThreadId)
            {
                sourceBreak = true;
            }

            if (!sourceBreak)
            {
                var request = new HttpRequestMessage(HttpMethod.Get, seg.Source.Url);
                ModSecret.SecretHeadersSign(seg.Source.Url, ref request, UseBrowserUserAgent, CustomUserAgent);

                if (!seg.IsFirstSegment || seg.StartPosition != 0)
                    request.Headers.Range = new System.Net.Http.Headers.RangeHeaderValue(seg.StartPosition, null);

                using (var cts = new CancellationTokenSource(timeout))
                using (var response = NetworkService.GetClient()
                    .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cts.Token)
                    .GetAwaiter().GetResult())
                {
                    Requester.EnsureSuccess(response);

                    if (State == NetState.Interrupted) { sourceBreak = true; }

                    if (!sourceBreak)
                    {
                        var redirected = response.RequestMessage.RequestUri;
                        if (redirected.OriginalString != seg.Source.Url)
                        {
                            ModBase.Log($"[Download] {LocalName} {seg.Id}#：重定向至 {redirected.OriginalString}");
                            seg.Source.Url = redirected.OriginalString;
                        }

                        contentLength = response.Content.Headers.ContentLength.GetValueOrDefault(-1);

                        if (contentLength == -1)
                        {
                            if (TotalSize > 1)
                            {
                                if (seg.StartPosition != 0)
                                {
                                    ModBase.Log($"[Download] {LocalName} {seg.Id}#：ContentLength 返回了 -1，视作不支持分段下载");
                                    notSupportRange = true;
                                }
                            }
                            else
                            {
                                TotalSize = -1;
                                IsUnknownSize = true;
                                ModBase.Log($"[Download] {LocalName} {seg.Id}#：文件大小未知");
                            }
                        }
                        else if (contentLength < 0)
                        {
                            throw new Exception($"获取片大小失败，结果为 {contentLength}。");
                        }
                        else if (seg.IsFirstSegment)
                        {
                        if (Check != null)
                        {
                            if (contentLength < Check.MinSize && Check.MinSize > 0)
                                throw new Exception($"文件大小不足，获取结果为 {contentLength}，要求至少为 {Check.MinSize}。");
                            if (contentLength != Check.ActualSize && Check.ActualSize > 0)
                                throw new Exception($"文件大小不一致，获取结果为 {contentLength}，要求必须为 {Check.ActualSize}。");
                        }
                            TotalSize = contentLength;
                            IsUnknownSize = false;
                            ModBase.Log($"[Download] {LocalName} {seg.Id}#：文件大小 {contentLength}");

                            if (contentLength > 50 * 1024 * 1024)
                                CheckDiskSpace(contentLength);
                        }
                        else if (TotalSize < 0)
                        {
                            throw new Exception("非首线程运行时，尚未获取文件大小");
                        }
                        else if (seg.StartPosition > 0 && contentLength == TotalSize)
                        {
                            notSupportRange = true;
                        }
                        else if (TotalSize - seg.StartPosition != contentLength)
                        {
                            throw new WebException(
                                $"获取到的分段大小不一致：Range 起始于 {seg.StartPosition}，" +
                                $"预期 ContentLength 为 {TotalSize - seg.StartPosition}，" +
                                $"返回 ContentLength 为 {contentLength}，总文件大小 {TotalSize}");
                        }

                        if (notSupportRange)
                        {
                            lock (LockSource)
                            {
                                if (OnceSources.Contains(seg.Source))
                                {
                                    sourceBreak = true;
                                }
                                else
                                {
                                    OnceSources.Add(seg.Source);
                                }
                            }
                            if (!sourceBreak)
                            {
                                throw new WebException(
                                    $"该下载源不支持分段下载：Range 起始于 {seg.StartPosition}，" +
                                    $"预期 ContentLength 为 {TotalSize - seg.StartPosition}，" +
                                    $"返回 ContentLength 为 {contentLength}，总文件大小 {TotalSize}");
                            }
                        }

                        if (!sourceBreak)
                        {
                            seg.State = NetState.Reading;
                            lock (LockState)
                            {
                                if (State < NetState.Reading) State = NetState.Reading;
                            }

                            if (IsNoSplit)
                            {
                                if (ShouldStartSingleSegmentInMemory)
                                {
                                    seg.TempFilePath = null;
                                    _smallFileCache = new MemoryStream();
                                }
                                else
                                {
                                    resultStream = OpenSegmentTempFile(seg);
                                }
                            }
                            else
                            {
                                resultStream = OpenSegmentTempFile(seg);
                            }

                            // Download loop
                            using (var httpStream = response.Content.ReadAsStreamAsync().GetAwaiter().GetResult())
                            {
                                const int bufferSize = 16384;
                                using var bufferOwner = MemoryPool<byte>.Shared.Rent(bufferSize);
                                var dataBuffer = bufferOwner.Memory.Span;

                                httpDataCount = httpStream.Read(dataBuffer);
                                seg.LastReceiveTime = TimeUtils.GetTimeTick();

                                while ((IsUnknownSize || seg.RemainingBytes > 0) &&
                                       httpDataCount > 0 && !ModBase.IsProgramEnded &&
                                       State < NetState.Merging && !seg.Source.IsFailed)
                                {
                                    DownloadSchedulerPolicy.WaitForAvailableSpeedBudget();

                                    var realDataCount = IsUnknownSize
                                        ? httpDataCount
                                        : Math.Min(httpDataCount, (int)seg.RemainingBytes);

                                    DownloadSchedulerPolicy.ConsumeSpeedBudget(realDataCount);

                                    var deltaTime = TimeUtils.GetTimeTick() - seg.LastReceiveTime;
                                    if (deltaTime > 1000000) deltaTime = 1;

                                    if (realDataCount > 0)
                                    {
                                        if (seg.DownloadedBytes == 0)
                                        {
                                            seg.State = NetState.Downloading;
                                            lock (LockState)
                                            {
                                                if (State < NetState.Downloading) State = NetState.Downloading;
                                            }
                                            lock (LockCount)
                                            {
                                                ConnectCount++;
                                                ConnectTime += TimeUtils.GetTimeTick() - seg.InitTime;
                                            }
                                        }
                                        lock (LockCount)
                                        {
                                            seg.Source.FailCount = 0;
                                            foreach (var task in Loaders)
                                                task.FailCount = 0;
                                        }

                                        ModNet.NetManager.DownloadDone += realDataCount;
                                        lock (LockDone)
                                        {
                                            DownloadedBytes += realDataCount;
                                        }
                                        seg.DownloadedBytes += realDataCount;

                                        var pendingBuffer = dataBuffer.Slice(0, realDataCount);
                                        if (IsNoSplit)
                                        {
                                            if (_smallFileCache != null)
                                            {
                                                _smallFileCache.Write(pendingBuffer);
                                                if (IsUnknownSize && _smallFileCache.Length > SmallFileMemoryLimit)
                                                    resultStream = SpillSingleSegmentBufferToDisk(seg);
                                            }
                                            else
                                            {
                                                resultStream ??= OpenSegmentTempFile(seg);
                                                resultStream.Write(pendingBuffer);
                                            }
                                        }
                                        else
                                            resultStream.Write(pendingBuffer);

                                        if (deltaTime > 1500 && deltaTime > realDataCount)
                                            throw new TimeoutException(
                                                $"由于速度过慢断开链接，下载 {realDataCount} B，消耗 {deltaTime} ms。");

                                        seg.LastReceiveTime = TimeUtils.GetTimeTick();

                                        if (seg.RemainingBytes == 0 && !IsUnknownSize) break;
                                    }
                                    else if (seg.LastReceiveTime > 0 && deltaTime > timeout)
                                    {
                                        throw new TimeoutException("操作超时，无数据。");
                                    }

                                    var readStart = TimeUtils.GetTimeTick();
                                    httpDataCount = httpStream.Read(dataBuffer);
                                    var readElapsed = TimeUtils.GetTimeTick() - readStart;
                                    if (readElapsed > timeout * 0.5 && httpDataCount == 0)
                                        throw new TimeoutException($"读取操作超时，耗时 {readElapsed}ms，返回数据量 {httpDataCount}。");
                                }
                            }
                        }
                    }
                }
            }

            // Finalize segment state
            if (sourceBreak || State == NetState.Interrupted || seg.Source.IsFailed ||
                (seg.RemainingBytes > 0 && !IsUnknownSize))
            {
                seg.State = NetState.Interrupted;
                ModBase.Log($"[Download] {LocalName} {seg.Id}#：中断");
            }
            else if (httpDataCount == 0 && seg.RemainingBytes > 0 && !IsUnknownSize)
            {
                throw new Exception(
                    $"返回的 ContentLength 过多：ContentLength 为 {contentLength}，" +
                    $"但获取到的总数据量仅为 {seg.DownloadedBytes}");
            }
            else
            {
                seg.State = NetState.Finished;
                if (ModBase.ModeDebug)
                    ModBase.Log($"[Download] {LocalName} {seg.Id}#：完成，已下载 {seg.DownloadedBytes}");
            }
        }
        catch (Exception exc)
        {
            ModBase.Log($"[Download] {LocalName}：出错，{(exc is TimeoutException or OperationCanceledException ? $"已超时（{timeout}ms）" : exc.Message)}");
            HandleSourceFail(seg, exc, false);
        }
        finally
        {
            resultStream?.Dispose();
            DownloadSchedulerPolicy.ReleaseThreadSlot();
            ApplyFinalizationAction(GetFinalizationActionAfterThreadExit(seg));
        }
    }

    #endregion

    #region Source Fail Handling

    private void HandleSourceFail(DownloadSegment seg, Exception ex, bool isMergeFailure)
    {
        FileFinalizationAction action = FileFinalizationAction.None;
        Exception actionError = null;

        lock (LockCount)
        {
            seg.Source.FailCount++;
            foreach (var task in Loaders)
                task.FailCount++;
        }

        if (IsNoSplit)
            ScheduleSingleSegmentRetryBackoff();

        seg.State = NetState.Interrupted;
        seg.Source.LastException = ex;

        var isRangeNotSupported = ex.Message.Contains("(416)");
        var shouldDisable = isMergeFailure || isRangeNotSupported ||
            ex.Message.Contains("(502)") || ex.Message.Contains("(404)") ||
            ex.Message.Contains("未能解析") || ex.Message.Contains("无返回数据") ||
            ex.Message.Contains("空间不足") ||
            ((ex.Message.Contains("(403)") || ex.Message.Contains("(429)")) &&
             !seg.Source.Url.Contains("bmclapi")) ||
            (seg.Source.FailCount >= Math.Clamp(ModNet.NetTaskThreadLimit, 5, 30) && DownloadedBytes < 1) ||
            seg.Source.FailCount > ModNet.NetTaskThreadLimit + 2;

        if (shouldDisable)
        {
            bool isThisFail = false;
            lock (LockSource)
            {
                if (!seg.Source.IsFailed || seg.Source.SingleThreadId == seg.Id)
                {
                    isThisFail = true;
                    seg.Source.IsFailed = true;
                }
            }

            if (isThisFail)
            {
                ModBase.Log($"[Download] {LocalName}：下载源被禁用（{seg.Source.Id}，Range 问题：{isRangeNotSupported}）：{seg.Source.Url}");
                lock (LockSource)
                {
                    OnceSources.Remove(seg.Source);
                }

                if (ex.Message.Contains("空间不足"))
                {
                    action = FileFinalizationAction.Fail;
                    actionError = ex;
                }
                else if (HasAvailableSource() && isMergeFailure)
                {
                    action = FileFinalizationAction.Retry;
                }
                else if (HasAvailableSource())
                {
                    // Continue with other sources
                }
                else if (!_retried)
                {
                    _retried = true;
                    lock (LockSource)
                    {
                        OnceSources.Clear();
                        foreach (var source in AllSources)
                        {
                            OnceSources.Add(source);
                            source.IsFailed = true;
                        }
                    }
                    action = FileFinalizationAction.Retry;
                }
                else
                {
                    ModBase.Log($"[Download] {LocalName}：已无可用下载源，下载失败");
                    Exception exampleEx = null;
                    lock (LockSource)
                    {
                        foreach (var source in AllSources)
                        {
                            if (source.LastException != null) exampleEx = source.LastException;
                        }
                    }
                    action = FileFinalizationAction.Fail;
                    actionError = exampleEx;
                }
            }
        }

        if (action == FileFinalizationAction.None && TotalSize == -2)
            action = FileFinalizationAction.Retry;

        ApplyFinalizationAction(action, actionError);
    }

    #endregion

    #region Merge

    public void Merge()
    {
        lock (LockState)
        {
            if (State < NetState.Merging)
                State = NetState.Merging;
            else
                return;
        }

        MergeCore();
    }

    private void MergeCore()
    {
        int retryCount = 0;
        Stream mergeFile = null;
    Retry:
        try
        {
            lock (LockChain)
            {
                if (File.Exists(LocalPath)) File.Delete(LocalPath);
                Directory.CreateDirectory(ModBase.GetPathFromFullPath(LocalPath));

                if (_smallFileCache != null)
                {
                    if (_smallFileCache == null)
                        throw new Exception($"小文件缓存为空，无法合并文件（{LocalName}）。");
                    _smallFileCache.Seek(0, SeekOrigin.Begin);
                    mergeFile = new FileStream(LocalPath, FileMode.Create);
                    _smallFileCache.CopyTo(mergeFile);
                    mergeFile.Dispose();
                    mergeFile = null;
                }
                else if (Segments?.Next == null && Segments?.TempFilePath != null)
                {
                    File.Copy(Segments.TempFilePath, LocalPath, overwrite: true);
                }
                else
                {
                    mergeFile = new FileStream(LocalPath, FileMode.Create);
                    for (var cur = Segments; cur != null; cur = cur.Next)
                    {
                        if (cur.DownloadedBytes == 0 || cur.TempFilePath == null) continue;
                        using var fs = new FileStream(cur.TempFilePath, FileMode.Open, FileAccess.Read, FileShare.Read);
                        fs.CopyTo(mergeFile);
                    }
                    mergeFile.Dispose();
                    mergeFile = null;
                }

                if (!IsUnknownSize && Check != null)
                {
                    if (Check.ActualSize == -1)
                        Check.ActualSize = TotalSize;
                    else if (Check.ActualSize != TotalSize)
                        throw new Exception($"文件大小不一致：任务要求为 {Check.ActualSize} B，网络获取结果为 {TotalSize}B");
                }

                var checkResult = Check?.Check(LocalPath);
                if (checkResult != null)
                    throw new Exception(checkResult);

                if (_smallFileCache != null)
                {
                    _smallFileCache?.Dispose();
                    _smallFileCache = null;
                }

                for (var cur = Segments; cur != null; cur = cur.Next)
                {
                    if (cur.TempFilePath != null) File.Delete(cur.TempFilePath);
                }

                Finish();
            }
        }
        catch (Exception ex)
        {
            ModBase.Log(ex, $"合并文件出错（{LocalName}）");
            mergeFile?.Dispose();
            mergeFile = null;
            if (retryCount <= 3)
            {
                Thread.Sleep(PCL.Core.Utils.RandomUtils.NextInt(500, 1000));
                retryCount++;
                goto Retry;
            }

            HandleMergeFailure(ex);
        }
    }

    #endregion

    #region State Transitions

    public void Finish(bool printLog = true)
    {
        lock (LockState)
        {
            if (State >= NetState.Finished) return;
            State = NetState.Finished;
        }
        ClearSingleSegmentRetryBackoff();
        lock (NetManager.Instance.LockRemain)
        {
            NetManager.Instance.FileRemain--;
            if (printLog)
                ModBase.Log($"[Download] {LocalName}：已完成，剩余文件 {NetManager.Instance.FileRemain}");
        }
        foreach (var task in Loaders)
            task.OnFileFinish(this);
    }

    public void Fail(Exception raiseEx = null)
    {
        lock (LockState)
        {
            if (State >= NetState.Finished) return;
            if (raiseEx != null) Errors.Add(raiseEx);
            State = NetState.Interrupted;
        }
        ClearSingleSegmentRetryBackoff();
        InterruptAndDelete();
        foreach (var task in Loaders)
            task.OnFileFail(this);
    }

    public void Abort(LoaderDownload causedByTask)
    {
        Loaders.Remove(causedByTask);
        if (Loaders.Count > 0) return;
        lock (LockState)
        {
            if (State >= NetState.Finished) return;
            State = NetState.Interrupted;
        }
        ClearSingleSegmentRetryBackoff();
        InterruptAndDelete();
    }

    private void InterruptAndDelete()
    {
        TryDeleteLocalFile();
        lock (NetManager.Instance.LockRemain)
        {
            NetManager.Instance.FileRemain--;
            ModBase.Log($"[Download] {LocalName}：状态 {State}，剩余文件 {NetManager.Instance.FileRemain}");
        }
    }

    public void Reset()
    {
        lock (LockChain)
        {
            for (var cur = Segments; cur != null; cur = cur.Next)
            {
                try { if (cur.TempFilePath != null && File.Exists(cur.TempFilePath)) File.Delete(cur.TempFilePath); }
                catch { /* ignore */ }
            }
        }
        _smallFileCache?.Dispose();
        _smallFileCache = null;
        Segments = null;
        lock (LockDone) { DownloadedBytes = 0; }
    }

    private void ApplyFinalizationAction(FileFinalizationAction action, Exception error = null)
    {
        // Centralize post-thread decisions here so the active C# downloader stays the single authority for merge/retry/fail.
        switch (action)
        {
            case FileFinalizationAction.None:
                return;
            case FileFinalizationAction.Merge:
                lock (LockState)
                {
                    if (State != NetState.Merging)
                        return;
                }
                MergeCore();
                return;
            case FileFinalizationAction.Retry:
                ResetForRetry();
                return;
            case FileFinalizationAction.Fail:
                Fail(error);
                return;
            default:
                throw new ArgumentOutOfRangeException(nameof(action), action, null);
        }
    }

    private FileFinalizationAction GetFinalizationActionAfterThreadExit(DownloadSegment seg)
    {
        lock (LockState)
        {
            if (State >= NetState.Merging || seg.State != NetState.Finished)
                return FileFinalizationAction.None;

            lock (LockChain)
            {
                if (!IsMergeReadyNoLock())
                    return FileFinalizationAction.None;

                State = NetState.Merging;
                return FileFinalizationAction.Merge;
            }
        }
    }

    private void HandleMergeFailure(Exception ex)
    {
        FileFinalizationAction action = FileFinalizationAction.None;
        Exception actionError = null;

        var isSpaceNotEnough = ex.Message.Contains("空间不足");

        if (isSpaceNotEnough)
        {
            action = FileFinalizationAction.Fail;
            actionError = ex;
        }
        else if (HasAvailableSource())
        {
            action = FileFinalizationAction.Retry;
        }
        else if (!_retried)
        {
            _retried = true;
            lock (LockSource)
            {
                OnceSources.Clear();
                foreach (var source in AllSources)
                    OnceSources.Add(source);
            }
            action = FileFinalizationAction.Retry;
        }
        else
        {
            action = FileFinalizationAction.Fail;
            actionError = ex;
        }

        if (action == FileFinalizationAction.None && TotalSize == -2)
            action = FileFinalizationAction.Retry;

        ApplyFinalizationAction(action, actionError);
    }

    private bool IsMergeReadyNoLock()
    {
        if (Segments == null)
            return false;

        for (var cur = Segments; cur != null; cur = cur.Next)
        {
            if (cur.State != NetState.Finished)
                return false;
        }

        return IsUnknownSize ? DownloadedBytes > 0 : TotalSize >= 0 && DownloadedBytes >= TotalSize;
    }

    private void ResetForRetry()
    {
        lock (LockState)
        {
            if (State >= NetState.Finished)
                return;
        }

        TryDeleteLocalFile();
        Reset();

        lock (LockState)
        {
            if (State >= NetState.Finished)
                return;

            State = NetState.WaitingToDownload;
        }
    }

    private int GetTimeoutMilliseconds(DownloadSegment seg)
    {
        var baseTimeout = Math.Max(AverageConnectTime, 12000);
        var retryMultiplier = 1 << Math.Min(seg.Source.FailCount, 2);
        return Math.Min(baseTimeout * retryMultiplier, 45000);
    }

    private void ScheduleSingleSegmentRetryBackoff()
    {
        var delay = GetSingleSegmentRetryDelayMilliseconds(_singleSegmentRetryCount);
        _singleSegmentRetryCount++;
        _nextSingleSegmentRetryAllowedTime = TimeUtils.GetTimeTick() + delay;
    }

    private void ClearSingleSegmentRetryBackoff()
    {
        _singleSegmentRetryCount = 0;
        _nextSingleSegmentRetryAllowedTime = 0;
    }

    private static int GetSingleSegmentRetryDelayMilliseconds(int retryCount)
    {
        var normalizedRetryCount = Math.Max(0, retryCount);
        var uncappedDelay = 1500 * (1 << Math.Min(normalizedRetryCount, 4));
        return Math.Min(uncappedDelay, 15000);
    }

    private void TryDeleteLocalFile()
    {
        try
        {
            if (File.Exists(LocalPath)) File.Delete(LocalPath);
        }
        catch (Exception ex)
        {
            ModBase.Log(ex, $"[Download] 尝试删除文件 {LocalPath} 失败，忽略错误");
        }
    }

    #endregion

    #region Disk Space Check

    private void CheckDiskSpace(long contentLength)
    {
        var tempRoot = TryGetLocalDriveRoot(ModBase.PathTemp);
        var localRoot = TryGetLocalDriveRoot(LocalPath);
        if (tempRoot == null || localRoot == null) return;

        foreach (var drive in DriveInfo.GetDrives())
        {
            if (!drive.IsReady) continue;
            long requiredSpace = 0;
            if (string.Equals(drive.Name, tempRoot, StringComparison.OrdinalIgnoreCase))
                requiredSpace += (long)(contentLength * 1.1);
            if (string.Equals(drive.Name, localRoot, StringComparison.OrdinalIgnoreCase))
                requiredSpace += contentLength + 5 * 1024 * 1024;
            if (requiredSpace > 0 && drive.TotalFreeSpace < requiredSpace)
            {
                var msg = $"{drive.Name.TrimEnd('\\')} 盘空间不足，无法进行下载。\r\n" +
                          $"需要至少 {ModBase.GetString(requiredSpace)} 空间，但当前仅剩余 {ModBase.GetString(drive.TotalFreeSpace)}。";
                throw new IOException(msg);
            }
        }
    }

    private FileStream OpenSegmentTempFile(DownloadSegment seg)
    {
        seg.TempFilePath ??= $"{ModBase.PathTemp}Download\\{Id}_{seg.Id}_{PCL.Core.Utils.RandomUtils.NextInt(0, 999999)}.tmp";
        return new FileStream(seg.TempFilePath, FileMode.Create, FileAccess.Write, FileShare.Read);
    }

    private Stream SpillSingleSegmentBufferToDisk(DownloadSegment seg)
    {
        if (_smallFileCache == null)
            throw new Exception($"小文件缓存为空，无法切换到磁盘缓冲（{LocalName}）。");

        var memoryCache = _smallFileCache;
        var tempFileStream = OpenSegmentTempFile(seg);
        memoryCache.Seek(0, SeekOrigin.Begin);
        memoryCache.CopyTo(tempFileStream);
        memoryCache.Dispose();
        _smallFileCache = null;
        return tempFileStream;
    }

    private static string TryGetLocalDriveRoot(string path)
    {
        if (string.IsNullOrEmpty(path)) return null;
        try
        {
            var root = Path.GetPathRoot(path);
            if (root?.Length == 3 && root[1] == ':' && root[2] == '\\')
                return root.ToUpperInvariant();
        }
        catch { }
        return null;
    }

    #endregion

    public override bool Equals(object obj) => obj is DownloadFile f && Id == f.Id;
    public override int GetHashCode() => Id;
}
