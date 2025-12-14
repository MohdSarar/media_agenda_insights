$ErrorActionPreference = "Stop"

# === CONFIG ===
$PROJECT_DIR = "C:\Users\Utilisateur\Desktop\media_agenda_insights"
$LOG_DIR = Join-Path $PROJECT_DIR "logs"
$LOCK_FILE = Join-Path $LOG_DIR "pipeline.lock"

if (!(Test-Path $LOG_DIR)) {
    New-Item -ItemType Directory -Path $LOG_DIR | Out-Null
}

$DATE = Get-Date -Format "yyyy-MM-dd"
$LOG_FILE = Join-Path $LOG_DIR "pipeline_$DATE.log"

function Log {
    param([string]$Message)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    "$timestamp | $Message" | Tee-Object -FilePath $LOG_FILE -Append
}

function Fail {
    param([string]$Step, [string]$Msg)
    Log "❌ FAIL: $Step"
    Log $Msg
    throw $Msg
}

# ---- Lock to prevent double runs ----
if (Test-Path $LOCK_FILE) {
    $lockAge = (Get-Date) - (Get-Item $LOCK_FILE).LastWriteTime
    Log "⚠️ Lock file exists ($LOCK_FILE). Age: $($lockAge.TotalMinutes.ToString('0.0')) min"
    Log "Aborting to avoid concurrent runs."
    exit 2
}

New-Item -ItemType File -Path $LOCK_FILE -Force | Out-Null

try {
    Set-Location $PROJECT_DIR

    Log "================ PIPELINE START ================="
    Log "User: $env:USERNAME | Host: $env:COMPUTERNAME"
    Log "Working directory: $PROJECT_DIR"
    Log "Python version:"
    python --version 2>&1 | Tee-Object -FilePath $LOG_FILE -Append

    function Run-Step {
        param([string]$Label, [string]$ModuleName)

        # Optional existence check: convert module -> relative .py path
        $relPath = ($ModuleName -replace '\.', '\') + ".py"
        $fullPath = Join-Path $PROJECT_DIR $relPath

        Log "---- START: $Label ----"
        Log "Command: python -m $ModuleName"

        if (!(Test-Path $fullPath)) {
            Log "⚠️ SKIP: module file not found: $fullPath"
            return
        }

        $start = Get-Date

        # temp output files
        $tmpOut = Join-Path $LOG_DIR ("tmp_" + [guid]::NewGuid().ToString() + ".out.txt")
        $tmpErr = Join-Path $LOG_DIR ("tmp_" + [guid]::NewGuid().ToString() + ".err.txt")

        $p = Start-Process -FilePath "python" `
            -ArgumentList @("-m", $ModuleName) `
            -NoNewWindow `
            -PassThru `
            -Wait `
            -RedirectStandardOutput $tmpOut `
            -RedirectStandardError $tmpErr

        $elapsed = (Get-Date) - $start
        Log "ExitCode: $($p.ExitCode) | Duration: $($elapsed.ToString())"

        if (Test-Path $tmpOut) {
            Get-Content $tmpOut | Tee-Object -FilePath $LOG_FILE -Append
            Remove-Item $tmpOut -Force
        }
        if (Test-Path $tmpErr) {
            Get-Content $tmpErr | Tee-Object -FilePath $LOG_FILE -Append
            Remove-Item $tmpErr -Force
        }

        if ($p.ExitCode -ne 0) {
            Fail $Label "Python exited with code $($p.ExitCode)."
        }

        Log "---- END: $Label (SUCCESS) ----"
    }

    # === PIPELINE ===
    Run-Step "Ingestion TV"                "ingestion.tv.ingest_tv"
    Run-Step "Ingestion Presse"            "ingestion.presse.ingest_press"
    Run-Step "Ingestion France24"          "ingestion.tv.ingest_france24"
    Run-Step "Ingestion Social Reddit"     "ingestion.social.ingest_reddit"

    Run-Step "NLP Global"                  "processing.nlp.process_articles"
    Run-Step "NLP France24"                "processing.nlp.process_france24_articles"
    Run-Step "NLP Social"                  "processing.nlp.process_social_posts"

    Run-Step "Keywords Global"             "processing.keywords.extract_keywords"
    Run-Step "Topics Global"               "processing.topics.extract_topics"

    Run-Step "Keywords France24"           "processing.keywords.extract_france24_keywords"
    Run-Step "Topics France24"             "processing.topics.extract_france24_topics"

    Run-Step "Keywords Social"             "processing.keywords.extract_social_keywords"
    Run-Step "Topics Social"               "processing.topics.extract_social_topics"

    Run-Step "Bias Analysis"               "processing.bias.analyze_topic_bias"
    Run-Step "Spike Detection"             "processing.spikes.detect_topic_spikes"

    Run-Step "Keyword Lifetime"            "processing.lifetime.keyword_lifetime"
    Run-Step "Topic Lifetime"              "processing.lifetime.topic_lifetime"
    Run-Step "Theme Lifetime"              "processing.lifetime.theme_lifetime"

    Log "================ PIPELINE END (SUCCESS) ================="
}
catch {
    Log "================ PIPELINE END (FAILED) ================="
    Log "PowerShell Exception:"
    Log ($_.Exception.Message)
    Log "StackTrace:"
    Log ($_.ScriptStackTrace)
    exit 1
}
finally {
    # Always remove lock (unless you want to keep it on failure)
    if (Test-Path $LOCK_FILE) {
        Remove-Item $LOCK_FILE -Force
    }
}
