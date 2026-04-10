using PCL.Core.Utils;

namespace PCL.Network.Engine;

using System;

public class DownloadSegment
{
    public int Id { get; set; }
    public long StartPosition { get; set; }
    public long DownloadedBytes { get; set; }
    public DownloadSource Source { get; set; }
    public NetState State { get; set; } = NetState.WaitingToDownload;
    public string TempFilePath { get; set; }
    public long InitTime { get; set; } = TimeUtils.GetTimeTick();
    public long LastReceiveTime { get; set; } = -1;
    public Exception Error { get; set; }

    // 用于链表结构
    public DownloadSegment Next { get; set; }

    public long EndPosition
    {
        get
        {
            if (Next == null)
                return ParentFile.IsUnknownSize ? 5L * 1024 * 1024 * 1024 : ParentFile.TotalSize - 1;
            return Next.StartPosition - 1;
        }
    }

    public long RemainingBytes => EndPosition - (StartPosition + DownloadedBytes) + 1;
    public bool IsFirstSegment => StartPosition == 0 && ParentFile.TotalSize == -2;
    public bool IsEnded => State == NetState.Finished || State == NetState.Interrupted;

    internal DownloadFile ParentFile { get; set; }
}

public enum NetState
{
    WaitingToCheck = -1,
    WaitingToDownload = 0,
    Connecting = 1,
    Reading = 2,
    Downloading = 3,
    Merging = 4,
    Finished = 5,
    Interrupted = 6
}

public enum NetPreDownloadBehaviour
{
    HintWhileExists,
    ExitWhileExistsOrDownloading,
    IgnoreCheck
}