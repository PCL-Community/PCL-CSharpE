namespace PCL.Network.Engine;

using System;

public class DownloadSource
{
    public int Id { get; set; }
    public required string Url { get; set; }
    public int FailCount { get; set; }
    public bool IsFailed { get; set; }
    public Exception? LastException { get; set; }

    /// <summary>
    /// 若该源被强制单线程下载，记录唯一的线程 ID
    /// </summary>
    public int? SingleThreadId { get; set; }

    public override string ToString() => Url;
}