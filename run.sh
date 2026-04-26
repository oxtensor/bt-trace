pm2 delete bt-trace
pm2 start "python main.py" --name bt-trace
pm2 log bt-trace