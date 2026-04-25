using System.Buffers;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Security.Policy;
using System.Text;
using System.Text.Json;
using PCL.Core.IO.Net;
using PCL.Core.Logging;
using PCL.Core.Utils;

namespace PCL.Network;

public static class Requester
{
    public static void EnsureSuccess(HttpResponseMessage response)
    {
        if (!response.IsSuccessStatusCode)
        {
            var content = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();
            response.Content?.Dispose();
            throw new HttpRequestFailedException(response, content);
        }
    }

    #region FetchString (simple GET with retry)

    public static async Task<string> FetchStringAsync(string url, RequestParam param = default)
    {
        int retryCount = 0;
        Exception retryException = null;
        var startTime = TimeUtils.GetTimeTick();

        while (retryCount <= 3)
        {
            retryCount++;
            try
            {
                var currentUrl = retryCount switch
                {
                    1 => url,
                    2 => param.FallbackUrl ?? url,
                    _ => param.FallbackUrl ?? url
                };
                var timeout = retryCount switch
                {
                    1 => 10000,
                    2 => 30000,
                    _ => 4000
                };

                if (retryCount > 2 && TimeUtils.GetTimeTick() - startTime <= 5500)
                    throw retryException;

                if (retryCount > 1) Thread.Sleep(500);

                return await FetchStringOnceAsync(currentUrl, param, timeout);
            }
            catch (ThreadInterruptedException)
            {
                throw;
            }
            catch (Exception ex)
            {
                retryException = ex;
            }
        }

        throw retryException;
    }

    private static async Task<string> FetchStringOnceAsync(string url, RequestParam param, int timeout)
    {
        url = ModSecret.SecretCdnSign(url);
        using var cts = new CancellationTokenSource(timeout);
        using var request = new HttpRequestMessage(HttpMethod.Get, url);

        if (param.Accept is not null) request.Headers.Accept.ParseAdd(param.Accept);

        var argClient = request;
        ModSecret.SecretHeadersSign(url, ref argClient, param.UseBrowserUserAgent);

        using var response = await NetworkService.GetClient()
            .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cts.Token);
        EnsureSuccess(response);

        using var responseStream = await response.Content.ReadAsStreamAsync();
        using var reader = new StreamReader(responseStream, param.Encoding ?? Encoding.UTF8);
        var content = await reader.ReadToEndAsync();
        if (string.IsNullOrEmpty(content))
            throw new WebException("获取结果失败，内容为空（" + url + "）");
        return content;
    }

    public static string FetchString(string url, RequestParam param = default) =>
        FetchStringAsync(url, param).GetAwaiter().GetResult();

    #endregion

    #region FetchJson

    public static async Task<object> FetchJsonAsync(string url, RequestParam param = default)
    {
        string result = await FetchStringAsync(url, param);
        return ModBase.GetJson(result);
    }

    public static async Task<T?> FetchJsonAsync<T>(string url, RequestParam param = default)
    {
        string result = await FetchStringAsync(url, param);
        return JsonSerializer.Deserialize<T>(result);
    }

    public static object FetchJson(string url, RequestParam param = default) =>
        FetchJsonAsync(url, param).GetAwaiter().GetResult();

    public static T? FetchJson<T>(string url, RequestParam param = default) =>
        FetchJsonAsync<T>(url, param).GetAwaiter().GetResult();

    #endregion

    #region Fetch (general HTTP request with retry)

    public static async Task<string> FetchAsync(string url, FetchParam param = default)
    {
        int retryCount = 0;
        Exception retryException = null;
        var startTime = TimeUtils.GetTimeTick();

        while (retryCount <= 3)
        {
            retryCount++;
            try
            {
                var currentUrl = retryCount switch
                {
                    1 => url,
                    2 => param.FallbackUrl ?? url,
                    _ => param.FallbackUrl ?? url
                };
                var timeout = retryCount switch
                {
                    1 => param.Timeout,
                    2 => 25000,
                    _ => 4000
                };

                if (retryCount > 2 && TimeUtils.GetTimeTick() - startTime <= 5500)
                    throw retryException;

                if (retryCount > 1) Thread.Sleep(500);

                return await FetchOnceAsync(currentUrl, param, timeout);
            }
            catch (ThreadInterruptedException)
            {
                throw;
            }
            catch (HttpRequestFailedException ex) when (
                param.DontRetryOnRefused &&
                ((int)ex.StatusCode).ToString().StartsWith('4'))
            {
                throw;
            }
            catch (Exception ex)
            {
                retryException = ex;
                if (param.MakeLog)
                    LogWrapper.Debug($"[Network] 网络请求第 {retryCount} 次失败（{url}）");
            }
        }

        throw retryException;
    }

    private static async Task<string> FetchOnceAsync(string url, FetchParam param, int timeout)
    {
        url = ModSecret.SecretCdnSign(url);
        if (param.MakeLog)
            LogWrapper.Info($"[Network] 发起网络请求: {param.Method} {url}, 超时 {timeout}ms");

        using var cts = new CancellationTokenSource(timeout);
        var method = ParseMethod(param.Method);
        using var request = new HttpRequestMessage(method, url);

        var argClient = request;
        ModSecret.SecretHeadersSign(url, ref argClient, param.UseBrowserUserAgent);

        if (param.Content != null && SupportBody(method))
        {
            request.Content = param.Content switch
            {
                byte[] bytes => new ByteArrayContent(bytes),
                string str => new StringContent(str, Encoding.UTF8, param.ContentType),
                HttpContent content => content,
                _ => throw new ArgumentException($"不支持的 Http Content 参数类型: {param.Content.GetType().Name}")
            };
        }

        if (param.Headers != null)
        {
            foreach (var pair in param.Headers.Where(p => !string.IsNullOrWhiteSpace(p.Key)))
            {
                if (request.Headers.Contains(pair.Key)) request.Headers.Remove(pair.Key);
                request.Headers.TryAddWithoutValidation(pair.Key, pair.Value);
            }
        }

        try
        {
            using var response = await NetworkService.GetClient()
                .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cts.Token);
            EnsureSuccess(response);
            return await response.Content.ReadAsStringAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            throw new TimeoutException($"[Network] 网络请求超时: {url}");
        }
        catch (HttpRequestFailedException ex)
        {
            throw new HttpWebException($"[Network] 网络请求失败: {url}", ex);
        }
        catch (Exception ex) when (ex is not HttpWebException)
        {
            throw new WebException($"[Network] 网络请求失败: {url}", ex);
        }
    }

    public static string Fetch(string url, FetchParam param = default) =>
        FetchAsync(url, param).GetAwaiter().GetResult();

    public static string Fetch(string url, string method, object content = null,
        string contentType = null, Dictionary<string, string> headers = null)
    {
        return Fetch(url, new FetchParam
        {
            Method = method,
            Content = content,
            ContentType = contentType,
            Headers = headers
        });
    }

    #endregion

    #region Helpers

    public static HttpMethod ParseMethod(string method) => method?.ToUpper() switch
    {
        "GET"     => HttpMethod.Get,
        "POST"    => HttpMethod.Post,
        "PUT"     => HttpMethod.Put,
        "DELETE"  => HttpMethod.Delete,
        "PATCH"   => HttpMethod.Patch,
        "HEAD"    => HttpMethod.Head,
        "OPTIONS" => HttpMethod.Options,
        "TRACE"   => HttpMethod.Trace,
        _         => throw new ArgumentException($"Unsupported method: {method}")
    };

    public static bool SupportBody(HttpMethod method)
    {
        return method == HttpMethod.Post ||
               method == HttpMethod.Put ||
               method == HttpMethod.Patch ||
               method == HttpMethod.Delete;
    }

    public static int Ping(string ip, int timeout = 10000, bool makeLog = true)
    {
        System.Net.NetworkInformation.PingReply result;
        try
        {
            result = new System.Net.NetworkInformation.Ping().Send(ip);
        }
        catch (Exception ex)
        {
            if (makeLog)
                ModBase.Log($"[Network] Ping {ip} 失败: {ex.Message}");
            return -1;
        }

        if (result.Status == System.Net.NetworkInformation.IPStatus.Success)
        {
            if (makeLog)
                ModBase.Log($"[Network] Ping {ip} 结束: {result.RoundtripTime}ms");
            return (int)result.RoundtripTime;
        }

        if (makeLog)
            ModBase.Log($"[Network] Ping {ip} 失败");
        return -1;
    }

    #endregion
}
