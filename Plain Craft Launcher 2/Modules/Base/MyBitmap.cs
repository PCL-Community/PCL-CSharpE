using PCL.Core.UI.Media;
using System.Collections.Concurrent;
using System.Drawing;
using System.Drawing.Drawing2D;
using System.Drawing.Imaging;
using System.IO;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using PixelFormat = System.Drawing.Imaging.PixelFormat;


namespace PCL;

/// <summary>
/// 一个万能的自动图片类型转换工具类
/// </summary>
public class MyBitmap
{
    private static readonly ConcurrentDictionary<string, MyBitmap> Cache = [];

    /// <summary>
    /// 存储的图片
    /// </summary>
    public readonly Bitmap Picture;

    #region Constructor

    /// <summary>
    /// Contructe <see cref="MyBitmap"/> with <see cref="filePathOrResName"/> argument
    /// </summary>
    /// <param name="filePathOrResName">The file path or resource name that used to contruct <see cref="MyBitmap"/></param>
    /// <exception cref="Exception">Throws when failed to load <see cref="MyBitmap"/>.</exception>
    /// <exception cref="InvalidDataException">Throws when image type is not supported or image is broken.</exception>
    public MyBitmap(string filePathOrResName)
    {
        try
        {
            filePathOrResName = filePathOrResName.Replace("pack://application:,,,/images/", ModBase.PathImage);
            if (filePathOrResName.StartsWithF(ModBase.PathImage))
            {
                if (Cache.TryGetValue(filePathOrResName, out var value))
                {
                    Picture = value.Picture;
                }
                else
                {
                    var conterted = ConvertToImageSource(filePathOrResName);

                    if (conterted is null)
                    {
                        throw new InvalidDataException("Cannot convert resource path to ImageSource");
                    }

                    Picture = new MyBitmap(conterted).Picture;
                    Cache.TryAdd(filePathOrResName, Picture);
                }
            }
            else
            {
                // 使用这种自己接管 FileStream 的方法加载才能解除文件占用
                using var picStream = new FileStream(filePathOrResName, FileMode.Open);
                if (picStream.Length > 2L && picStream.ReadByte() == 82 && picStream.ReadByte() == 73)
                {
                    picStream.Seek(0L, SeekOrigin.Begin);
                    // 调用 WIC 转换，需要系统内置 WebP 组件，专治各种精简系统
                    using var ms = picStream.FromWebpToPng();
                    Picture = new Bitmap(ms);
                }
                else
                {
                    Picture = new Bitmap(picStream);
                }
            }
        }
        catch (Exception ex)
        {
            Picture = (Bitmap)System.Windows.Application.Current.TryFindResource(filePathOrResName);
            if (Picture is null)
            {
                Picture = new Bitmap(1, 1);
                if (ex is ArgumentException)
                {
                    throw new InvalidDataException($"图片格式不支持，或图片文件损坏（{filePathOrResName}）", ex);
                }

                throw new Exception($"加载 MyBitmap 意外失败（{filePathOrResName}）", ex);
            }

            ModBase.Log(ex, $"指定类型有误的 MyBitmap 加载（{filePathOrResName}）", ModBase.LogLevel.Developer);
        }
    }

    /// <summary>
    /// Contruct <see cref="MyBitmap"/> from <see cref="ImageSource"/>
    /// </summary>
    public MyBitmap(ImageSource image)
    {
        using var ms = new MemoryStream();
        var encoder = new PngBitmapEncoder();
        encoder.Frames.Add(BitmapFrame.Create((BitmapSource)image));
        encoder.Save(ms);
        Picture = new Bitmap(ms);
    }

    /// <summary>
    /// Construct <see cref="MyBitmap"/> from <see cref="Image"/>
    /// </summary>
    public MyBitmap(Image image)
    {
        Picture = (Bitmap)image;
    }

    /// <summary>
    /// Construct <see cref="MyBitmap"/> from <see cref="Bitmap"/>
    /// </summary>
    public MyBitmap(Bitmap image)
    {
        Picture = image;
    }

    /// <summary>
    /// Construct <see cref="MyBitmap"/> from <see cref="ImageBrush"/>
    /// </summary>
    public MyBitmap(ImageBrush image)
    {
        using var ms = new MemoryStream();
        var encoder = new BmpBitmapEncoder();
        encoder.Frames.Add(BitmapFrame.Create((BitmapSource)image.ImageSource));
        encoder.Save(ms);
        Picture = new Bitmap(ms);
    }

    #endregion

    #region Implicit Converter

    /// <summary>
    /// Convert <see cref="Image"/> to <see cref="MyBitmap"/>
    /// </summary>
    public static implicit operator MyBitmap(Image? image)
    {
        ArgumentNullException.ThrowIfNull(image);
        return new MyBitmap(image);
    }

    /// <summary>
    /// Convert <see cref="MyBitmap"/> to <see cref="Image"/>
    /// </summary>
    public static implicit operator Image(MyBitmap? image)
    {
        ArgumentNullException.ThrowIfNull(image);
        return image.Picture;
    }

    /// <summary>
    /// Convert <see cref="ImageSource"/> to <see cref="MyBitmap"/>
    /// </summary>
    public static implicit operator MyBitmap(ImageSource? image)
    {
        ArgumentNullException.ThrowIfNull(image);
        return new MyBitmap(image);
    }

    /// <summary>
    /// Convert <see cref="MyBitmap"/> to <see cref="ImageSource"/>
    /// </summary>
    public static implicit operator ImageSource(MyBitmap? image)
    {
        ArgumentNullException.ThrowIfNull(image);

        var bitmapPic = image.Picture;
        var rect = new Rectangle(0, 0, bitmapPic.Width, bitmapPic.Height);
        var bitmapData = bitmapPic.LockBits(rect, ImageLockMode.ReadWrite, PixelFormat.Format32bppArgb);
        try
        {
            var result = BitmapSource.Create(bitmapPic.Width,
                bitmapPic.Height,
                bitmapPic.HorizontalResolution,
                bitmapPic.VerticalResolution,
                PixelFormats.Bgra32,
                null,
                bitmapData.Scan0,
                rect.Width * rect.Height * 4,
                bitmapData.Stride);

            result.Freeze();
            return result;
        }
        finally
        {
            bitmapPic.UnlockBits(bitmapData);
        }
    }

    /// <summary>
    /// Convert <see cref="Bitmap"/> to <see cref="MyBitmap"/>
    /// </summary>
    public static implicit operator MyBitmap(Bitmap? image)
    {
        ArgumentNullException.ThrowIfNull(image);
        return new MyBitmap(image);
    }

    /// <summary>
    /// Convert <see cref="MyBitmap"/> to <see cref="Bitmap"/>
    /// </summary>
    public static implicit operator Bitmap(MyBitmap? image)
    {
        ArgumentNullException.ThrowIfNull(image);
        return image.Picture;
    }

    /// <summary>
    /// Convert <see cref="ImageBrush"/> to <see cref="MyBitmap"/>
    /// </summary>
    public static implicit operator MyBitmap(ImageBrush? image)
    {
        ArgumentNullException.ThrowIfNull(image);
        return new MyBitmap(image);
    }

    /// <summary>
    /// Convert <see cref="MyBitmap"/> to <see cref="ImageBrush"/>
    /// </summary>
    public static implicit operator ImageBrush(MyBitmap? image)
    {
        ArgumentNullException.ThrowIfNull(image);
        return new ImageBrush(new MyBitmap(image.Picture));
    }

    #endregion

    /// <summary>
    /// 获取裁切的图片
    /// </summary>
    /// <remarks>
    /// 这个方法不会导致原对象改变，而是会返回一个新的对象。
    /// </remarks>
    public MyBitmap Clip(int x, int y, int width, int height)
    {
        var bitmap = new Bitmap(width, height, Picture.PixelFormat);
        bitmap.SetResolution(Picture.HorizontalResolution, Picture.VerticalResolution);
        using var graph = Graphics.FromImage(bitmap);
        graph.InterpolationMode = InterpolationMode.NearestNeighbor;
        graph.TranslateTransform(-x, -y);
        graph.DrawImage(Picture, new Rectangle(0, 0, Picture.Width, Picture.Height));

        return bitmap;
    }

    /// <summary>
    /// 获取旋转或翻转后的图片
    /// </summary>
    /// <remarks>
    /// 这个方法不会导致原对象改变，而是会返回一个新的对象。
    /// </remarks>
    public MyBitmap RotateFlip(RotateFlipType type)
    {
        var bitmap = new Bitmap(Picture);
        bitmap.SetResolution(Picture.HorizontalResolution, Picture.VerticalResolution);
        bitmap.RotateFlip(type);
        return bitmap;
    }

    /// <summary>
    /// 将图像保存到文件。
    /// </summary>
    public void Save(string filePath)
    {
        BitmapEncoder encoder = new PngBitmapEncoder();
        encoder.Frames.Add(BitmapFrame.Create((BitmapSource)this));
        using var fileStream = new FileStream(filePath, FileMode.Create);
        encoder.Save(fileStream);
    }

    private static readonly ImageSourceConverter ImageSourceConverter = new();

    private ImageSource? ConvertToImageSource(string val)
    {
        return ImageSourceConverter.ConvertFromString(val) as ImageSource;
    }
}