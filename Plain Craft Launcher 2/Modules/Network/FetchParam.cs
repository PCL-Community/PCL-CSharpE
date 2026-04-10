using System.Net.Http;
using System.Text;

namespace PCL.Network;

public readonly record struct FetchParam(
    string Method = "GET",
    object? Content = null,
    string? ContentType = null,
    Dictionary<string, string>? Headers = null,
    Encoding? Encoding = null,
    string? Accept = null,
    string? FallbackUrl = null,
    bool UseBrowserUserAgent = false,
    bool MakeLog = true,
    bool DontRetryOnRefused = true,
    int Timeout = 30000
);
