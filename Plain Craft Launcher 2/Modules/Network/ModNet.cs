using System.Text;
using PCL.Network.Engine;
using PCL.Network.Loaders;

namespace PCL.Network;

public static class ModNet
{
    // Authority note: the compiled downloader evolution path lives in Modules\Network\*.cs.
    // Modules\Base\ModNet.vb remains historical/reference material only and must not regain runtime authority.
    public const string NetDownloadEnd = ".PCLDownloading";

    // Global scheduler knobs and counters are configured here, but scheduling decisions should flow through DownloadSchedulerPolicy.
    public static int NetTaskThreadLimit { get; set; }
    public static long NetTaskSpeedLimitLow { get; set; } = 256 * 1024L;
    public static long NetTaskSpeedLimitHigh { get; set; } = -1;

    private static long _netTaskSpeedLimitLeft = -1;
    public static long NetTaskSpeedLimitLeft
    {
        get => _netTaskSpeedLimitLeft;
        set => _netTaskSpeedLimitLeft = value;
    }

    private static int _netTaskThreadCount;
    public static int NetTaskThreadCount
    {
        get => _netTaskThreadCount;
        set => _netTaskThreadCount = value;
    }

    public static readonly object LockThreadCount = new();
    public static readonly object LockSpeedLimitLeft = new();

    public static NetManager NetManager => NetManager.Instance;

    #region Facade: NetGetCodeByRequestRetry

    public static object NetGetCodeByRequestRetry(string url, Encoding encode = null,
        string accept = "", bool IsJson = false, string backupUrl = null,
        bool useBrowserUserAgent = false)
    {
        var param = new RequestParam
        {
            Encoding = encode,
            Accept = string.IsNullOrEmpty(accept) ? null : accept,
            FallbackUrl = backupUrl,
            UseBrowserUserAgent = useBrowserUserAgent,
            Timeout = 30000,
            Retries = 3
        };
        var result = Requester.FetchString(url, param);
        return IsJson ? ModBase.GetJson(result) : result;
    }

    #endregion

    #region Facade: NetGetCodeByRequestOnce

    public static object NetGetCodeByRequestOnce(string url, Encoding encode = null,
        int timeout = 30000, bool isJson = false, string accept = "",
        bool useBrowserUserAgent = false)
    {
        var param = new RequestParam
        {
            Encoding = encode,
            Accept = string.IsNullOrEmpty(accept) ? null : accept,
            UseBrowserUserAgent = useBrowserUserAgent,
            Timeout = timeout,
            Retries = 0
        };
        var result = Requester.FetchString(url, param);
        return isJson ? ModBase.GetJson(result) : result;
    }

    #endregion

    #region Facade: NetGetCodeByLoader

    public static string NetGetCodeByLoader(string url, int timeout = 45000,
        bool isJson = false, bool useBrowserUserAgent = false)
    {
        return NetGetCodeByLoader(new[] { url }, timeout, isJson, useBrowserUserAgent);
    }

    public static string NetGetCodeByLoader(IEnumerable<string> urls, int timeout = 45000,
        bool IsJson = false, bool useBrowserUserAgent = false)
    {
        var temp = ModBase.PathTemp + "download.txt";
        var task = new LoaderDownload("源码获取 " + ModBase.GetUuid() + "#",
            new List<DownloadFile>
            {
                new(urls, temp, new ModBase.FileChecker { IsJson = IsJson }, useBrowserUserAgent)
            });
        try
        {
            task.WaitForExitTime(timeout, TimeoutMessage: "连接服务器超时");
            var content = ModBase.ReadFile(temp);
            System.IO.File.Delete(temp);
            return content;
        }
        finally
        {
            task.Abort();
        }
    }

    #endregion

    #region Facade: NetRequestRetry / NetRequestOnce

    public static string NetRequestRetry(string url, string method, object data,
        string contentType, bool dontRetryOnRefused = true,
        Dictionary<string, string> headers = null)
    {
        var param = new FetchParam
        {
            Method = method,
            Content = data,
            ContentType = contentType,
            Headers = headers,
            DontRetryOnRefused = dontRetryOnRefused,
            Timeout = 25000
        };
        return Requester.Fetch(url, param);
    }

    public static string NetRequestOnce(string url, string method, object data,
        string contentType, int timeout = 25000,
        Dictionary<string, string> headers = null, bool makeLog = true,
        bool useBrowserUserAgent = false)
    {
        var param = new FetchParam
        {
            Method = method,
            Content = data,
            ContentType = contentType,
            Headers = headers,
            UseBrowserUserAgent = useBrowserUserAgent,
            MakeLog = makeLog,
            Timeout = timeout
        };
        return Requester.Fetch(url, param);
    }

    #endregion

    #region Facade: Download

    // Keep direct-download helpers on the maintained C# path below. Legacy VB downloader code is reference-only.
    public static Task NetDownloadByClient(string url, string localFile,
        bool useBrowserUserAgent = false)
    {
        return FileDownloader.Download(url, localFile, useBrowserUserAgent);
    }

    public static void NetDownloadByLoader(string url, string localFile,
        ModLoader.LoaderBase loaderToSyncProgress = null,
        ModBase.FileChecker check = null, bool useBrowserUserAgent = false)
    {
        FileDownloader.DownloadByLoader(url, localFile, loaderToSyncProgress, check, useBrowserUserAgent);
    }

    public static void NetDownloadByLoader(IEnumerable<string> urls, string localFile,
        ModLoader.LoaderBase loaderToSyncProgress = null,
        ModBase.FileChecker check = null, bool useBrowserUserAgent = false)
    {
        FileDownloader.DownloadByLoader(urls, localFile, loaderToSyncProgress, check, useBrowserUserAgent);
    }

    #endregion

    public static bool HasDownloadingTask(bool ignoreCustomDownload = false)
    {
        foreach (var task in ModLoader.LoaderTaskbar.ToList())
        {
            if (task.Show && task.State == ModBase.LoadState.Loading &&
                (!ignoreCustomDownload || !task.Name.ToString().Contains("自定义下载")))
                return true;
        }
        return false;
    }
}
