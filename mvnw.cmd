@ECHO OFF
SETLOCAL
SET "MAVEN_VERSION=3.9.9"
SET "WRAPPER_DIR=%~dp0.mvn\wrapper\apache-maven-%MAVEN_VERSION%"
SET "MAVEN_BIN=%WRAPPER_DIR%\bin\mvn.cmd"
IF NOT EXIST "%MAVEN_BIN%" (
  ECHO Maven %MAVEN_VERSION% is not installed in %WRAPPER_DIR%.
  ECHO Install Maven or place its distribution there, then run this command again.
  EXIT /B 1
)
CALL "%MAVEN_BIN%" %*
