using System.IO;
using System.Net;
using System.Net.Http;
using PCL.Core.IO.Net;
using PCL.Core.Logging;
using PCL.Network.Engine;
using PCL.Network.Loaders;

namespace PCL.Network;

public class FileDownloader
{
    public static async Task Download(string url, string filePath, bool useBrowserUserAgent = false)
    {
        ModBase.Log("[Network] 直接下载文件：" + url);
        try
        {
            Directory.CreateDirectory(ModBase.GetPathFromFullPath(filePath));
            if (File.Exists(filePath))
                File.Delete(filePath);
            using var request = new HttpRequestMessage(HttpMethod.Get, url);
            var argClient = request;
            ModSecret.SecretHeadersSign(url, ref argClient, useBrowserUserAgent);
            using var response = await NetworkService.GetClient()
                .SendAsync(request, HttpCompletionOption.ResponseHeadersRead);
            Requester.EnsureSuccess(response);
            using var httpStream = await response.Content.ReadAsStreamAsync();
            using var fileStream = new FileStream(filePath, FileMode.Create);
            await httpStream.CopyToAsync(fileStream);
        }
        catch (TaskCanceledException ex) when (ex.InnerException is null)
        {
            throw new TimeoutException($"下载超时（{url}）", ex);
        }
        catch (HttpRequestFailedException ex)
        {
            throw new HttpWebException($"下载失败：{ex.Message}（{url}）", ex);
        }
        catch (Exception ex)
        {
            if (File.Exists(filePath))
                File.Delete(filePath);
            throw new WebException($"下载失败：{ex.Message}（{url}）", ex);
        }
    }

    public static void DownloadByLoader(string url, string filePath,
        ModLoader.LoaderBase loader = null, ModBase.FileChecker check = null,
        bool useBrowserUserAgent = false)
    {
        DownloadByLoader(new[] { url }, filePath, loader, check, useBrowserUserAgent);
    }

    public static void DownloadByLoader(IEnumerable<string> urls, string filePath,
        ModLoader.LoaderBase loader = null, ModBase.FileChecker check = null,
        bool useBrowserUserAgent = false)
    {
        var task = new LoaderDownload("文件下载 " + ModBase.GetUuid() + "#",
            new List<DownloadFile>
            {
                new(urls, filePath, check, useBrowserUserAgent)
            });
        try
        {
            task.WaitForExit(LoaderToSyncProgress: loader);
        }
        catch (Exception ex)
        {
            throw new WebException($"多线程直接下载文件失败（第一下载源：{urls.First()}）", ex);
        }
        finally
        {
            task.Abort();
        }
    }
}
