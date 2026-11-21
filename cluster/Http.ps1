if(-not ('GarlicCluster.HttpDeadline' -as [type])){Add-Type -Path (Join-Path $PSScriptRoot 'HttpDeadline.cs')}
function Invoke-ClusterHttp($Node,[string]$Method,[string]$Path,[string]$Body='',[int]$Timeout=5,[scriptblock]$OnProgress=$null) {
    if($Timeout -lt 1 -or $Timeout -gt 600){throw 'HTTP timeout must be 1-600 seconds.'}
    $key=[Environment]::GetEnvironmentVariable($Node.keyEnvironment)
    if([string]::IsNullOrWhiteSpace($key)){throw 'Set the node API key environment variable before connecting.'}
    $request=[Net.HttpWebRequest]::Create("http://$($Node.address):$($Node.port)$Path")
    $request.Proxy=$null;$request.AllowAutoRedirect=$false;$request.Method=$Method
    $request.Timeout=$Timeout*1000;$request.ReadWriteTimeout=$Timeout*1000
    $request.Headers['Authorization']='Bearer '+$key
    $deadline=New-Object GarlicCluster.HttpDeadline($request,($Timeout*1000))
    $response=$null;$reader=$null
    try{
        if($Method -eq 'POST'){
            $bytes=[Text.Encoding]::UTF8.GetBytes($Body)
            $request.ContentType='application/json';$request.ContentLength=$bytes.Length
            $stream=$request.GetRequestStream();try{$stream.Write($bytes,0,$bytes.Length)}finally{$stream.Dispose()}
        }
        $response=$request.GetResponse()
        if([int]$response.StatusCode -ne 200){throw 'Unexpected HTTP status.'}
        $reader=New-Object IO.StreamReader($response.GetResponseStream(),(New-Object Text.UTF8Encoding($false,$true)),$false)
        if($OnProgress){
            if($response.ContentType -notlike 'text/event-stream*'){throw 'Expected SSE response.'}
            return Read-ClusterSse $reader $OnProgress $Timeout
        }
        if($response.ContentType -notlike 'application/json*'){throw 'Expected JSON response.'}
        $buffer=New-Object char[] 4096;$text=New-Object Text.StringBuilder
        while(($read=$reader.Read($buffer,0,$buffer.Length)) -gt 0){
            if($text.Length+$read -gt 2097152){throw 'Response exceeds read limits.'}
            $null=$text.Append($buffer,0,$read)
        }
        $text.ToString() | ConvertFrom-Json
    }catch [Net.WebException]{
        if($_.Exception.Response){$_.Exception.Response.Dispose()}
        throw
    }finally{
        if($reader){$reader.Dispose()};if($response){$response.Dispose()}
        $deadline.Dispose();$request.Abort()
    }
}
