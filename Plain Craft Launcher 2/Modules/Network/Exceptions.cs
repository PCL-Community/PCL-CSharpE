using System.Net;
using System.Net.Http;

namespace PCL.Network;

public class HttpRequestFailedException : HttpRequestException
{
    public new HttpStatusCode StatusCode { get; }
    public string ReasonPhrase { get; }
    public HttpResponseMessage Response { get; }
    public string WebResponse { get; }

    public HttpRequestFailedException(HttpResponseMessage response, string webResponse = null)
        : base($"HTTP 响应失败: {response.ReasonPhrase} ({(int)response.StatusCode})")
    {
        Response = response;
        StatusCode = response.StatusCode;
        ReasonPhrase = response.ReasonPhrase;
        WebResponse = webResponse;
    }
}

public class HttpWebException : WebException
{
    public HttpRequestFailedException InnerHttpException { get; }
    public HttpStatusCode StatusCode => InnerHttpException.StatusCode;

    public HttpWebException(string message, HttpRequestFailedException inner)
        : base(message, inner)
    {
        InnerHttpException = inner;
    }
}
