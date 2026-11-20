function Read-ClusterSse($Reader,[scriptblock]$OnProgress,[int]$Timeout=180) {
    $deadline=[datetime]::UtcNow.AddSeconds($Timeout)
    $text=New-Object Text.StringBuilder;$line=New-Object Text.StringBuilder
    $data=New-Object 'Collections.Generic.List[string]'
    $finish=$null;$done=$false;$total=0
    while(-not $done){
        $c=$Reader.Read()
        if($c -lt 0){break}
        $total++
        if($total -gt 4194304 -or [datetime]::UtcNow -gt $deadline){throw 'Stream exceeds size/time limits.'}
        if($c -ne 10){if($line.Length -ge 262144){throw 'SSE line too long.'};$null=$line.Append([char]$c);continue}
        $value=$line.ToString().TrimEnd([char]13);$null=$line.Clear()
        if($value.StartsWith('data:')){$data.Add($value.Substring(5).TrimStart(' '));continue}
        if($value -ne ''){continue}
        if(-not $data.Count){continue}
        $payload=$data -join "`n";$data.Clear()
        if($payload -eq '[DONE]'){$done=$true;break}
        $event=$payload | ConvertFrom-Json
        if($event.PSObject.Properties['error']){throw 'Server reported streaming error.'}
        if(-not $event.PSObject.Properties['choices'] -or @($event.choices).Count -ne 1){throw 'Invalid stream choice.'}
        $choice=$event.choices[0]
        if($choice.index -ne 0){throw 'Unexpected choice index.'}
        if($choice.delta.PSObject.Properties['content'] -and $null -ne $choice.delta.content){
            if($finish -or $choice.delta.content -isnot [string]){throw 'Invalid stream ordering/content.'}
            $null=$text.Append($choice.delta.content)
            if($text.Length -gt 2097152){throw 'Stream content too large.'}
            if($OnProgress){$null=& $OnProgress $text.ToString()}
        }
        if($choice.PSObject.Properties['finish_reason'] -and $null -ne $choice.finish_reason){
            if($finish -or $choice.finish_reason -notin @('stop','length')){throw 'Invalid finish reason.'}
            $finish=$choice.finish_reason
        }
    }
    if(-not $done -or -not $finish){throw 'Stream ended without finish reason and DONE; completion is uncertain.'}
    [pscustomobject]@{choices=@([pscustomobject]@{message=[pscustomobject]@{role='assistant';content=$text.ToString()};finish_reason=$finish})}
}
function Get-ClusterProgress([string]$StateDirectory,[string]$JobId){
    if($JobId -notmatch '^[a-f0-9]{32}$'){throw 'Invalid job ID.'}
    $path=Join-Path $StateDirectory ($JobId+'.progress')
    if(Test-Path -LiteralPath $path){Get-Content -LiteralPath $path -Raw -Encoding UTF8 | ConvertFrom-Json}
}
function Get-ClusterMessages($Records,[string]$ConversationId,[string]$Prompt,[int]$MaxTokens,[int]$ContextTokens){
    $messages=@()
    if($ConversationId){
        if($ConversationId -notmatch '^[a-zA-Z0-9-]{1,64}$'){throw 'Conversation ID must contain 1-64 letters, numbers or hyphens.'}
        $turns=@($Records | Where-Object {$_.PSObject.Properties['conversation'] -and $_.conversation -eq $ConversationId} | Sort-Object created,id)
        if(@($turns | Where-Object state -in @('queued','running','unknown','resolved')).Count){throw 'Conversation has pending or uncertain history. Finish/cancel pending work, or use a new conversation ID after reconciliation.'}
        foreach($turn in $turns | Where-Object state -eq 'succeeded'){
            $messages+=@{role='user';content=$turn.prompt}
            $messages+=@{role='assistant';content=$turn.response.choices[0].message.content}
        }
    }
    $messages+=@{role='user';content=$Prompt}
    # Conservative byte upper bound, plus role/template overhead; not an exact tokenizer.
    $bound=64+$MaxTokens
    foreach($m in $messages){$bound += [Text.Encoding]::UTF8.GetByteCount($m.content)+32}
    if($bound -gt $ContextTokens){throw 'Conversation exceeds conservative context budget. Start a new conversation or reduce prompt/output size. Nothing was truncated.'}
    return ,$messages
}
