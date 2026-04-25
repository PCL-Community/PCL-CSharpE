namespace PCL.Network.Scheduling;

public static class DownloadSchedulerPolicy
{
    public const long AdaptiveLowSpeedFloorCap = 4L * 1024 * 1024;

    public static bool HasThreadBudget()
    {
        return ModNet.NetTaskThreadCount < ModNet.NetTaskThreadLimit;
    }

    public static long ClampAdaptiveLowSpeedFloor(long rawLimit)
    {
        return Math.Max(0, Math.Min(rawLimit, AdaptiveLowSpeedFloorCap));
    }

    public static bool TryReserveThreadSlot()
    {
        lock (ModNet.LockThreadCount)
        {
            if (ModNet.NetTaskThreadCount >= ModNet.NetTaskThreadLimit)
                return false;

            ModNet.NetTaskThreadCount++;
            return true;
        }
    }

    public static void ReleaseThreadSlot()
    {
        lock (ModNet.LockThreadCount)
        {
            if (ModNet.NetTaskThreadCount > 0)
                ModNet.NetTaskThreadCount--;
        }
    }

    public static bool ShouldExpandOngoingFile(long currentSpeed)
    {
        return currentSpeed < ModNet.NetTaskSpeedLimitLow;
    }

    public static bool CanStartAdditionalSegment(int preparingCount, int downloadingCount)
    {
        return preparingCount <= downloadingCount;
    }

    public static int GetPostStartDelayMilliseconds(string url)
    {
        return url.Contains("bmclapi") ? 100 : 0;
    }

    public static void RefillSpeedBudgetForNextTick()
    {
        if (ModNet.NetTaskSpeedLimitHigh > 0)
            ModNet.NetTaskSpeedLimitLeft = ModNet.NetTaskSpeedLimitHigh / 10;
    }

    public static void WaitForAvailableSpeedBudget()
    {
        while (ModNet.NetTaskSpeedLimitHigh > 0 && ModNet.NetTaskSpeedLimitLeft <= 0)
            Thread.Sleep(8);
    }

    public static void ConsumeSpeedBudget(int bytes)
    {
        if (bytes <= 0 || ModNet.NetTaskSpeedLimitHigh <= 0)
            return;

        lock (ModNet.LockSpeedLimitLeft)
        {
            if (ModNet.NetTaskSpeedLimitHigh > 0)
                ModNet.NetTaskSpeedLimitLeft -= bytes;
        }
    }
}
