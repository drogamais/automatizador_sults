@echo off
:: Vai para a pasta do arquivo
cd /d "%~dp0"

:: --- MUDANÇA AQUI: Agora salva com extensão .log ---
set ARQUIVO_LOG=log_execucao.log

:: --- Escreve o Cabeçalho no Log ---
echo. >> %ARQUIVO_LOG%
echo ================================================ >> %ARQUIVO_LOG%
echo Execucao iniciada em: %DATE% as %TIME% >> %ARQUIVO_LOG%
echo ================================================ >> %ARQUIVO_LOG%

:: --- Executa e salva Saida + Erros no .log ---
".\venv\Scripts\python.exe" "Sults Updater.py" >> %ARQUIVO_LOG% 2>&1

:: --- Registra o fim ---
echo. >> %ARQUIVO_LOG%
echo Finalizado em: %DATE% as %TIME% >> %ARQUIVO_LOG%

echo Processo finalizado. Verifique o arquivo %ARQUIVO_LOG% para detalhes.
pause