$path = Split-Path $MyInvocation.MyCommand.Path
$rootDir = (Get-Item $path).parent.FullName
$outDir = "$rootDir\out"

$requirements = "$rootDir\requirements.txt"
$python = (Get-Command "python").Source
$pythonDir = Resolve-Path $python
$venvName = "venv"


function Invoke-Command-With-Error-Handling([string]$command)
{
    Write-Output "Invoking: $command"
    Invoke-Expression $command
    if($LASTEXITCODE -ne 0)
    {
        throw "$command failed with exit code $LASTEXITCODE"
    }
}

function Create-VirtualEnvironment([string]$name)
{
    # Create the virtual environment
    Invoke-Command-With-Error-Handling "$python -m virtualenv $name"
}


Write-Output "----- Configuration -----"
Write-Output "RootDir:  $rootDir"
Write-Output "OutDir:   $outDir"
Write-Output "Python:   $python"

md -Force -Path $outDir
pushd $outDir

Create-VirtualEnvironment $venvName
Invoke-Command-With-Error-Handling "$venvName\Scripts\activate.ps1"

# Install our requirements
Invoke-Command-With-Error-Handling "cmd /c where python"
Invoke-Command-With-Error-Handling "cmd /c where pip"

Invoke-Command-With-Error-Handling "python -m pip install -r $requirements"

Write-Output "Deactivating build venv"
deactivate
popd

