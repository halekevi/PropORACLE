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

    if (-not ("PropOracleWin32Move" -as [type])) {
        Add-Type -TypeDefinition @"
using System;
using System.Runtime.InteropServices;
public static class PropOracleWin32Move {
  [DllImport("kernel32.dll")] public static extern IntPtr GetConsoleWindow();
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

    $hwnd = [PropOracleWin32Move]::GetConsoleWindow()
    if ($hwnd -eq [IntPtr]::Zero) { return }

    $rect = New-Object PropOracleWin32Move+RECT
    [void][PropOracleWin32Move]::GetWindowRect($hwnd, [ref]$rect)
    $w = [Math]::Max(640, $rect.Right - $rect.Left)
    $h = [Math]::Max(400, $rect.Bottom - $rect.Top)

    $wa = $target.WorkingArea
    $x = $wa.X + 40
    $y = $wa.Y + 40
    # Keep fully on the target working area
    if (($x + $w) -gt ($wa.X + $wa.Width)) { $x = $wa.X + [Math]::Max(0, $wa.Width - $w - 20) }
    if (($y + $h) -gt ($wa.Y + $wa.Height)) { $y = $wa.Y + [Math]::Max(0, $wa.Height - $h - 20) }

    [void][PropOracleWin32Move]::SetWindowPos(
        $hwnd,
        [IntPtr]::Zero,
        [int]$x,
        [int]$y,
        [int]$w,
        [int]$h,
        [PropOracleWin32Move]::SWP_NOZORDER -bor [PropOracleWin32Move]::SWP_SHOWWINDOW
    )
}
