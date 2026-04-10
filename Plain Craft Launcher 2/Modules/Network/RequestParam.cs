using System.Net.Http;
using System.Text;

namespace PCL.Network;

public readonly record struct RequestParam(
    Encoding? Encoding = null,
    string? Accept = null,
    string? FallbackUrl = null,
    bool UseBrowserUserAgent = false,
    int Timeout = 30000,
    int Retries = 0
)
{
    public static RequestParam WithRetry => new RequestParam
    {
        Retries = 3
    };
}