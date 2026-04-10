using System.IO;

namespace PCL.Network.Loaders;

/// <summary>
///     下载单个 UNC 文件的加载器。
/// </summary>
public class LoaderDownloadUnc : ModLoader.LoaderBase
{
    /// <summary>
    ///     下载线程。
    /// </summary>
    private Thread DownloadingThread;

    /// <summary>
    ///     保存路径。
    /// </summary>
    public string SavePath;

    /// <summary>
    ///     UNC 路径。
    /// </summary>
    public string Unc;

    public LoaderDownloadUnc(string Name, Tuple<string, string> File)
    {
        this.Name = Name;
        Unc = File.Item1;
        SavePath = File.Item2;
    }

    public override void Start(object Input = null, bool IsForceRestart = false)
    {
        if (Input is not null)
        {
            Unc = Convert.ToString(((dynamic)Input).Item1);
            SavePath = Convert.ToString(((dynamic)Input).Item2);
        }

        State = ModBase.LoadState.Loading;
        Directory.CreateDirectory(ModBase.GetPathFromFullPath(SavePath));
        DownloadingThread = ModBase.RunInNewThread(DownloadThread, "Download UNC File");
    }

    private void DownloadThread()
    {
        try
        {
            var fileInfo = new FileInfo(Unc);
            var totalBytes = fileInfo.Length;
            var bytesRead = 0L;

            var tempFile = ModBase.PathTemp + Uuid + @"\" + ModBase.GetFileNameFromPath(SavePath);
            Directory.CreateDirectory(ModBase.GetPathFromFullPath(tempFile));
            if (File.Exists(tempFile))
                File.Delete(tempFile);
            using (var sourceStream = new FileStream(Unc, FileMode.Open, FileAccess.Read))
            {
                using (var destStream = new FileStream(tempFile, FileMode.Create, FileAccess.Write))
                {
                    var buffer = new byte[81921]; // 80KB 缓冲区
                    int currentBytesRead;

                    do
                    {
                        currentBytesRead = sourceStream.Read(buffer, 0, buffer.Length);
                        destStream.Write(buffer, 0, currentBytesRead);
                        bytesRead += currentBytesRead;

                        Progress = bytesRead / (double)totalBytes;
                    } while (currentBytesRead > 0 && State == ModBase.LoadState.Loading);
                }
            }

            if (State > ModBase.LoadState.Loading)
                return;
            ModBase.CopyFile(tempFile, SavePath);
            if (State == ModBase.LoadState.Loading)
                State = ModBase.LoadState.Finished;
        }
        catch (ThreadAbortException ex)
        {
        }
    }

    public override void Abort()
    {
        if (State >= ModBase.LoadState.Finished)
            return;
        State = ModBase.LoadState.Aborted;
        ModBase.Log("[Download] " + Name + " 已取消！");
    }
}