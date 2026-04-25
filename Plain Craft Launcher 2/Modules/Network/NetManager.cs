using System.IO;
using PCL.Core.Logging;
using PCL.Core.Utils;
using PCL.Network.Engine;
using PCL.Network.Loaders;
using PCL.Network.Scheduling;

namespace PCL.Network;

public class NetManager
{
    private static readonly Lazy<NetManager> _instance = new(() => new NetManager());
    public static NetManager Instance => _instance.Value;

    private NetManager() { }

    public Dictionary<string, DownloadFile> Files = new();
    public readonly object LockFiles = new();

    public ModBase.SafeList<LoaderDownload> Tasks = new();

    public long DownloadDone
    {
        get => _downloadDone;
        set { lock (LockDone) { _downloadDone = value; } }
    }

    public readonly object LockRemain = new();
    public int FileRemain;

    private long _downloadDone;
    private readonly object LockDone = new();

    private long _refreshStatLast;
    private readonly List<long> _speedHistory = new();
    private long _speedLastDone;
    private bool _downloadCacheCleared;

    public long Speed { get; private set; }
    public readonly int Uuid = ModBase.GetUuid();

    private bool _managerStarted;

    public void Start(LoaderDownload task)
    {
        EnsureManagerStarted();

        if (!_downloadCacheCleared)
        {
            try { ModBase.DeleteDirectory(ModBase.PathTemp + "Download"); }
            catch (Exception ex) { LogWrapper.Warn(ex, "清理下载缓存失败"); }
            _downloadCacheCleared = true;
        }
        Directory.CreateDirectory(ModBase.PathTemp + "Download");

        lock (LockFiles)
        {
            for (int i = 0; i < task.Files.Count; i++)
            {
                var file = task.Files[i];
                if (Files.ContainsKey(file.LocalPath))
                {
                    if (Files[file.LocalPath].State >= NetState.Finished)
                    {
                        file.Loaders.Add(task);
                        Files[file.LocalPath] = file;
                        lock (LockRemain)
                        {
                            FileRemain++;
                            if (ModBase.ModeDebug)
                                ModBase.Log($"[Download] {file.LocalName}：已替换列表，剩余文件 {FileRemain}");
                        }
                    }
                    else
                    {
                        file = Files[file.LocalPath];
                        file.Loaders.Add(task);
                    }
                }
                else
                {
                    file.Loaders.Add(task);
                    Files.Add(file.LocalPath, file);
                    lock (LockRemain)
                    {
                        FileRemain++;
                        if (ModBase.ModeDebug)
                            ModBase.Log($"[Download] {file.LocalName}：已加入列表，剩余文件 {FileRemain}");
                    }
                }
                task.Files[i] = file;
            }
        }
        Tasks.Add(task);
    }

    private void EnsureManagerStarted()
    {
        if (_managerStarted) return;
        _managerStarted = true;

        StartThreadStarter(0);
        StartThreadStarter(1);
        StartStatRefresher();
    }

    private void StartThreadStarter(int id)
    {
        ModBase.RunInNewThread(() =>
        {
            try
            {
                while (true)
                {
                    Thread.Sleep(20);

                    List<DownloadFile> allFiles;
                    lock (LockFiles)
                    {
                        if (id == 0 && FileRemain == 0 && Files.Count > 0) Files.Clear();
                        allFiles = Files.Values.ToList();
                    }

                    var waitingFiles = new List<DownloadFile>();
                    var ongoingFiles = new List<DownloadFile>();

                    foreach (var file in allFiles)
                    {
                        if (file.Id % 2 == id) continue;
                        if (file.State == NetState.WaitingToDownload)
                            waitingFiles.Add(file);
                        else if (file.State < NetState.Merging)
                            ongoingFiles.Add(file);
                    }

                    foreach (var file in waitingFiles)
                    {
                        if (!DownloadSchedulerPolicy.HasThreadBudget()) break;
                        var newSeg = file.TryBeginThread();
                        var delay = newSeg == null ? 0 : DownloadSchedulerPolicy.GetPostStartDelayMilliseconds(newSeg.Source.Url);
                        if (delay > 0)
                            Thread.Sleep(delay);
                    }

                    if (!DownloadSchedulerPolicy.ShouldExpandOngoingFile(Speed)) continue;

                    foreach (var file in ongoingFiles)
                    {
                        if (!DownloadSchedulerPolicy.HasThreadBudget()) break;
                        int preparingCount = 0, downloadingCount = 0;
                        for (var cur = file.Segments; cur != null; cur = cur.Next)
                        {
                            if (cur.State < NetState.Downloading) preparingCount++;
                            else if (cur.State == NetState.Downloading) downloadingCount++;
                        }
                        if (!DownloadSchedulerPolicy.CanStartAdditionalSegment(preparingCount, downloadingCount)) continue;
                        var newSeg = file.TryBeginThread();
                        var delay = newSeg == null ? 0 : DownloadSchedulerPolicy.GetPostStartDelayMilliseconds(newSeg.Source.Url);
                        if (delay > 0)
                            Thread.Sleep(delay);
                    }
                }
            }
            catch (Exception ex)
            {
                LogWrapper.Error(ex, $"任务管理启动线程 {id} 出错");
            }
        }, $"NetManager ThreadStarter {id}");
    }

    private void StartStatRefresher()
    {
        _refreshStatLast = TimeUtils.GetTimeTick();
        _speedLastDone = 0;

        ModBase.RunInNewThread(() =>
        {
            try
            {
                var nextTick = TimeUtils.GetTimeTick();
                while (true)
                {
                    DownloadSchedulerPolicy.RefillSpeedBudgetForNextTick();

                    RefreshStat();

                    nextTick += 100;
                    var sleepTime = nextTick - TimeUtils.GetTimeTick();
                    if (sleepTime > 0)
                        Thread.Sleep((int)sleepTime);
                    else
                        nextTick = TimeUtils.GetTimeTick();
                }
            }
            catch (Exception ex)
            {
                LogWrapper.Error(ex, "任务管理刷新线程出错");
            }
        }, "NetManager StatRefresher");
    }

    private void RefreshStat()
    {
        try
        {
            var deltaTime = TimeUtils.GetTimeTick() - _refreshStatLast;
            if (deltaTime == 0) return;
            _refreshStatLast += deltaTime;

            var actualSpeed = Math.Max(0, (DownloadDone - _speedLastDone) / (deltaTime / 1000.0));
            _speedHistory.Insert(0, (long)actualSpeed);
            if (_speedHistory.Count >= 31) _speedHistory.RemoveAt(30);
            _speedLastDone = DownloadDone;

            long speedSum = 0, speedDiv = 0;
            long firstTenSpeedSum = 0;
            var weight = _speedHistory.Count;
            for (var i = 0; i < _speedHistory.Count; i++)
            {
                var record = _speedHistory[i];
                speedSum += record * weight;
                speedDiv += weight;
                if (i < 10)
                    firstTenSpeedSum += record;
                weight--;
            }
            Speed = speedDiv > 0 ? speedSum / speedDiv : 0;

            if (_speedHistory.Count >= 10)
            {
                var rawLimit = (long)(firstTenSpeedSum / 10.0 * 0.85);
                var limit = DownloadSchedulerPolicy.ClampAdaptiveLowSpeedFloor(rawLimit);
                if (limit > ModNet.NetTaskSpeedLimitLow)
                {
                    ModNet.NetTaskSpeedLimitLow = limit;
                    ModBase.Log($"[Download] 速度下限已提升到 {ModBase.GetString(limit)}");
                }
            }

            foreach (var task in Tasks)
                task.RefreshStat();
        }
        catch (Exception ex)
        {
            LogWrapper.Warn(ex, "刷新下载公开属性失败");
        }
    }
}
