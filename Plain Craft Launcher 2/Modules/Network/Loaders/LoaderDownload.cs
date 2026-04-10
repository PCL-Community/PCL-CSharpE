using System.IO;
using PCL.Core.Logging;
using PCL.Core.Utils.Exts;
using PCL.Network.Engine;

namespace PCL.Network.Loaders;

public class LoaderDownload : ModLoader.LoaderBase
{
    public ModBase.SafeList<DownloadFile> Files;
    private int _fileRemain;
    private readonly object _fileRemainLock = new();
    private double _progress;

    public override double Progress
    {
        get
        {
            if (State >= ModBase.LoadState.Finished) return 1;
            if (!Files.Any()) return 0;
            return _progress;
        }
        set { _progress = value; }
    }

    public int FailCount { get; set; }

    public LoaderDownload(string name, List<DownloadFile> fileTasks)
    {
        Name = name;
        Files = new ModBase.SafeList<DownloadFile>(fileTasks);
    }

    public void RefreshStat()
    {
        double newProgress = 0;
        double totalProgress = 0;
        foreach (var file in Files)
        {
            if (file.IsCopy)
            {
                newProgress += file.Progress * 0.2;
                totalProgress += 0.2;
            }
            else
            {
                newProgress += file.Progress;
                totalProgress += 1;
            }
        }
        if (totalProgress > 0 && !double.IsNaN(totalProgress))
            newProgress /= totalProgress;
        _progress = newProgress;
    }

    public override void Start(object input = null, bool isForceRestart = false)
    {
        if (input != null)
            Files = new ModBase.SafeList<DownloadFile>((List<DownloadFile>)input);

        Files = new ModBase.SafeList<DownloadFile>(
            Files.Distinct((a, b) => a.LocalPath == b.LocalPath).ToList());

        lock (_fileRemainLock)
        {
            _fileRemain += Files.Count(f => f.State != NetState.Finished);
        }
        State = ModBase.LoadState.Loading;

        ModBase.RunInNewThread(() =>
        {
            try
            {
                if (!Files.Any())
                {
                    OnFinish();
                    return;
                }

                foreach (var file in Files)
                {
                    if (file == null)
                        throw new ArgumentException("存在空文件请求！");

                    foreach (var source in file.AllSources)
                    {
                        if (!source.Url.StartsWith("https://", StringComparison.OrdinalIgnoreCase) &&
                            !source.Url.StartsWith("http://", StringComparison.OrdinalIgnoreCase))
                        {
                            source.LastException = new ArgumentException("输入的下载链接不正确！");
                            source.IsFailed = true;
                        }
                    }
                    if (!file.HasAvailableSource())
                        throw new ArgumentException("输入的下载链接不正确！");

                    file.LocalPath = file.LocalPath.Replace("/", "\\");
                    if (!file.LocalPath.Contains(":\\", StringComparison.OrdinalIgnoreCase))
                        throw new ArgumentException("输入的本地文件地址不正确: " + file.LocalPath);
                    if (file.LocalPath.EndsWith("\\"))
                        throw new ArgumentException("请输入含文件名的完整文件路径: " + file.LocalPath);
                    Directory.CreateDirectory(ModBase.GetPathFromFullPath(file.LocalPath));
                }

                NetManager.Instance.Start(this);

                var filesToCheck = new List<DownloadFile>();
                var disabledCopy = false;

                foreach (var file in Files)
                {
                    if (!disabledCopy && file.Check?.CanUseExistsFile == true)
                        filesToCheck.Add(file);
                    else
                    {
                        lock (file.LockState)
                        {
                            file.State = NetState.WaitingToDownload;
                            file.IsCopy = false;
                        }
                    }
                }

                if (filesToCheck.Count > 0)
                    CheckExistingFiles(filesToCheck);
            }
            catch (Exception ex)
            {
                OnFail(new List<Exception> { new Exception("下载初始化失败", ex) });
            }
        }, $"L/下载 {Uuid}");
    }

    private void CheckExistingFiles(List<DownloadFile> files)
    {
        try
        {
            var folders = new List<string>
            {
                Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData) + "\\.minecraft\\"
            };

            foreach (var file in files)
            {
                var target = CheckExistingFile(folders, file);
                if (file.State >= NetState.WaitingToDownload) return;

                if (target == null)
                {
                    lock (file.LockState)
                    {
                        file.State = NetState.WaitingToDownload;
                        file.IsCopy = false;
                    }
                }
                else
                {
                    file.IsCopy = true;
                    int retryCount = 0;
                Retry:
                    try
                    {
                        if (target != file.LocalPath)
                        {
                            ModBase.Log($"[Download] 复制已存在的文件：{target} → {file.LocalPath}");
                            ModBase.CopyFile(target, file.LocalPath);
                        }
                        file.Finish(false);
                    }
                    catch (Exception ex)
                    {
                        retryCount++;
                        ModBase.Log(ex, $"复制已存在的文件失败，第 {retryCount} 次重试（{target} → {file.LocalPath}）");
                        if (retryCount < 3)
                        {
                            Thread.Sleep(200);
                            goto Retry;
                        }
                        lock (file.LockState)
                        {
                            file.State = NetState.WaitingToDownload;
                            file.IsCopy = false;
                        }
                    }
                }
            }
        }
        catch (Exception ex)
        {
            OnFail(new List<Exception> { new Exception("下载已存在文件查找失败", ex) });
        }
    }

    private string CheckExistingFile(List<string> folders, DownloadFile file)
    {
        if (file.Check.Check(file.LocalPath) == null) return file.LocalPath;
        if (file.Check.Hash == null && file.Check.ActualSize < 0) return null;

        var typeIndexes = new[]
        {
            "\\assets\\", "\\libraries\\", "\\versions\\", "\\mods\\",
            "\\coremods\\", "\\lib\\", "\\resourcepacks\\", "\\texturepacks\\", "\\shaderpacks\\"
        }.Select(folderName => (FolderName: folderName, Index: file.LocalPath.IndexOf(folderName, StringComparison.OrdinalIgnoreCase)))
         .Where(kv => kv.Index >= 0).ToList();

        if (typeIndexes.Count == 0)
        {
            if (file.LocalName.EndsWith(".jar", StringComparison.OrdinalIgnoreCase))
                typeIndexes.Add(("\\versions\\", 1));
            else
                return null;
        }

        var type = typeIndexes.MaxOrDefault(kv => kv.Index).FolderName.TrimStart('\\');
        var localAfterType = file.LocalPath.Substring(file.LocalPath.IndexOf(type, StringComparison.OrdinalIgnoreCase) + type.Length);

        switch (type)
        {
            case "assets\\":
            case "libraries\\":
                foreach (var folder in folders)
                {
                    var candidate = folder + type + localAfterType;
                    if (file.Check.Check(candidate) == null) return candidate;
                }
                break;
            case "versions\\":
                foreach (var folder in folders)
                {
                    var versionsFolder = folder + "versions\\";
                    if (!Directory.Exists(versionsFolder)) continue;
                    foreach (var versionDir in Directory.GetDirectories(versionsFolder))
                    {
                        var ext = Path.GetExtension(file.LocalName).ToLower();
                        foreach (var candidate in Directory.GetFiles(versionDir, "*" + ext, SearchOption.TopDirectoryOnly))
                        {
                            if (file.Check.Check(candidate) == null) return candidate;
                        }
                    }
                }
                break;
            default:
                if (file.Check.ActualSize < 0 || file.Check.Hash == null) return null;
                foreach (var folder in folders)
                {
                    var targetFolder = folder + type;
                    if (!Directory.Exists(targetFolder)) continue;
                    foreach (var candidate in Directory.GetFiles(targetFolder))
                    {
                        if (new FileInfo(candidate).Length != file.Check.ActualSize) continue;
                        if (file.Check.Check(candidate) == null) return candidate;
                    }
                }
                break;
        }
        return null;
    }

    public void OnFileFinish(DownloadFile file)
    {
        lock (_fileRemainLock)
        {
            _fileRemain--;
            if (_fileRemain > 0) return;
        }
        OnFinish();
    }

    public void OnFinish()
    {
        RaisePreviewFinish();
        lock (LockState)
        {
            if (State > ModBase.LoadState.Loading) return;
            State = ModBase.LoadState.Finished;
        }
    }

    public void OnFileFail(DownloadFile file)
    {
        foreach (var source in file.AllSources)
        {
            if (source.LastException != null)
                file.Errors.Add(source.LastException);
        }
        OnFail(file.Errors);
    }

    public void OnFail(List<Exception> exList)
    {
        lock (LockState)
        {
            if (State > ModBase.LoadState.Loading) return;
            if (exList == null || exList.Count == 0)
                exList = new List<Exception> { new Exception("未知错误！") };

            var usefulExs = exList.Where(e => !e.Message.Contains("404 (")).ToList();
            Error = usefulExs.Count > 0 ? usefulExs[0] : exList[0];

            foreach (var file in Files)
            {
                if (file.State == NetState.Interrupted)
                {
                    Error = new Exception(
                        "文件下载失败：" + file.LocalPath + "\r\n" +
                        string.Join("\r\n", file.AllSources.Select(s =>
                            s.LastException != null
                                ? s.LastException.Message + "（" + s.Url + "）"
                                : s.Url)), Error);
                    break;
                }
            }
            State = ModBase.LoadState.Failed;
        }

        foreach (var taskFile in Files)
        {
            if (taskFile.State < NetState.Merging)
                taskFile.State = NetState.Interrupted;
        }

        var errOutput = exList.Select(ex => ex.Message).Distinct().ToList();
        ModBase.Log("[Download] " + string.Join("\r\n", errOutput));
    }

    public override void Abort()
    {
        lock (LockState)
        {
            if (State >= ModBase.LoadState.Finished) return;
            State = ModBase.LoadState.Aborted;
        }
        ModBase.Log("[Download] " + Name + " 已取消！");
        foreach (var taskFile in Files)
            taskFile.Abort(this);
    }
}
