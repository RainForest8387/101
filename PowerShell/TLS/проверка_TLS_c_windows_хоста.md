Выполнить срипт проверки в PS

```powershell
$brokers = @(
    @{ Host = "kfk-tst-al-qm01.mcb.ru"; Port = 9094 },
    @{ Host = "kfk-tst-al-qm02.mcb.ru"; Port = 9094 },
    @{ Host = "kfk-tst-al-qm03.mcb.ru"; Port = 9094 }
)

$results = foreach ($b in $brokers) {
    $hostName = $b.Host
    $port = [int]$b.Port
    $tcp = $null
    $sslStream = $null
    $remoteCert = $null
    $status = "FAIL"
    $errorText = $null
    $elapsed = $null

    try {
        $sw = [System.Diagnostics.Stopwatch]::StartNew()

        $tcp = [Net.Sockets.TcpClient]::new()
        $tcp.Connect($hostName, $port)

        $stream = $tcp.GetStream()

        $callback = [Net.Security.RemoteCertificateValidationCallback]{
            param($sender, $certificate, $chain, $sslPolicyErrors)
            return $true
        }

        $sslStream = [Net.Security.SslStream]::new($stream, $false, $callback)

        $sslStream.AuthenticateAsClient($hostName)

        $remoteCert = $sslStream.RemoteCertificate
        $status = "OK"
        $elapsed = $sw.ElapsedMilliseconds
    }
    catch {
        $errorText = $_.Exception.Message
        $elapsed = $sw.ElapsedMilliseconds
    }
    finally {
        if ($sslStream) { $sslStream.Dispose() }
        if ($tcp) { $tcp.Close() }
    }

    [pscustomobject]@{
        Host      = $hostName
        Port      = $port
        Status    = $status
        TimeMs    = $elapsed
        Error     = $errorText
        CertSubj  = if ($remoteCert) { $remoteCert.Subject } else { $null }
        CertIssuer = if ($remoteCert) { $remoteCert.Issuer } else { $null }
        CertNotAfter = if ($remoteCert) { $remoteCert.GetExpirationDateString() } else { $null }
    }
}

$results | Format-Table -AutoSize
```
