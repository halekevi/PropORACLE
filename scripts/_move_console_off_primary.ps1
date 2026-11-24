#requires -Version 5.1
<#
.SYNOPSIS
  Move the current console window off the primary (usually gaming) monitor
  onto another display so scheduled PropOracle jobs don't cover the game.
#>
function Move-ConsoleOffPrimaryMonitor {
    try {
        Add-Type -AssemblyName System.Windows.Forms -ErrorAction Stop
    } catch {
        return
    }

    $screens = [System.Windows.Forms.Screen]::AllScreens
    if ($null -eq $screens -or $screens.Count -lt 2) { return }

    $primary = $screens | Where-Object { $_.Primary } | Select-Object -First 1
    if (-not $primary) { $primary = $screens[0] }

    # Prefer a display to the left of primary; else any non-primary.
    $alts = @($screens | Where-Object { -not $_.Primary })
    if ($alts.Count -eq 0) { return }
    $target = $alts |
        Sort-Object { $_.WorkingArea.X }, { $_.WorkingArea.Y } |
        Select-Object -First 1

    if (-not ("PropOracleWin32Move2" -as [type])) {
        Add-Type -TypeDefinition @"
using System;
using System.Runtime.InteropServices;
public static class PropOracleWin32Move2 {
  [DllImport("kernel32.dll")] public static extern IntPtr GetConsoleWindow();
  [DllImport("user32.dll")] public static extern IntPtr GetAncestor(IntPtr hWnd, uint flags);
  [DllImport("user32.dll")] public static extern bool IsZoomed(IntPtr hWnd);
  [DllImport("user32.dll")] public static extern bool ShowWindow(IntPtr hWnd, int cmd);
  [DllImport("user32.dll")] public static extern bool GetWindowRect(IntPtr hWnd, out RECT r);
  [DllImport("user32.dll")] public static extern bool SetWindowPos(
      IntPtr hWnd, IntPtr hWndInsertAfter, int X, int Y, int cx, int cy, uint uFlags);
  [StructLayout(LayoutKind.Sequential)]
  public struct RECT { public int Left; public int Top; public int Right; public int Bottom; }
  public const uint SWP_NOSIZE = 0x0001;
  public const uint SWP_NOZORDER = 0x0004;
  public const uint SWP_SHOWWINDOW = 0x0040;
}
"@
    }

    $hwnd = [PropOracleWin32Move2]::GetConsoleWindow()
    if ($hwnd -eq [IntPtr]::Zero) { return }
    # Under Windows Terminal the console HWND is a hidden ConPTY stub owned by the
    # visible terminal window; move the owner instead.
    $owner = [PropOracleWin32Move2]::GetAncestor($hwnd, 3)
    if ($owner -ne [IntPtr]::Zero -and $owner -ne $hwnd) { $hwnd = $owner }
    if ([PropOracleWin32Move2]::IsZoomed($hwnd)) { [void][PropOracleWin32Move2]::ShowWindow($hwnd, 9) }

    $rect = New-Object PropOracleWin32Move2+RECT
    [void][PropOracleWin32Move2]::GetWindowRect($hwnd, [ref]$rect)
    $w = [Math]::Max(640, $rect.Right - $rect.Left)
    $h = [Math]::Max(400, $rect.Bottom - $rect.Top)

    $wa = $target.WorkingArea
    $x = $wa.X + 40
    $y = $wa.Y + 40
    # Keep fully on the target working area
    if (($x + $w) -gt ($wa.X + $wa.Width)) { $x = $wa.X + [Math]::Max(0, $wa.Width - $w - 20) }
    if (($y + $h) -gt ($wa.Y + $wa.Height)) { $y = $wa.Y + [Math]::Max(0, $wa.Height - $h - 20) }

    [void][PropOracleWin32Move2]::SetWindowPos(
        $hwnd,
        [IntPtr]::Zero,
        [int]$x,
        [int]$y,
        [int]$w,
        [int]$h,
        [PropOracleWin32Move2]::SWP_NOZORDER -bor [PropOracleWin32Move2]::SWP_SHOWWINDOW
    )
}
