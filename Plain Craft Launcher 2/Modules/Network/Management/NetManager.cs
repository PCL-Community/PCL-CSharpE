namespace PCL.Network;

public sealed class NetManager
{
    private static readonly Lazy<NetManager> _instance = new(() => new NetManager());
    public static NetManager Instance => _instance.Value;

    public Dictionary<string, DownloadFile> Files { get; } = new();
    public object LockFiles { get; } = new();
    public ModBase.SafeList<PCL.Network.Loaders.LoaderDownload> Tasks { get; } = new();
    public object LockRemain { get; } = new();
    public int FileRemain;
    private long _downloadDone;
    public object LockDone { get; } = new();
    public long DownloadDone
    {
        get
        {
            lock (LockDone)
                return _downloadDone;
        }
        set
        {
            lock (LockDone)
                _downloadDone = value;
        }
    }

    public long Speed => Tasks.Where(t => t.State == ModBase.LoadState.Loading)
        .SelectMany(t => t.Files)
        .Sum(f => f.Speed);

    public void Start(PCL.Network.Loaders.LoaderDownload task)
    {
        lock (LockFiles)
        {
            Tasks.Add(task);
            foreach (var file in task.Files)
                Files[file.LocalPath] = file;
            FileRemain = Files.Values.Count(file => file.State != NetState.Finished);
        }
    }
}
