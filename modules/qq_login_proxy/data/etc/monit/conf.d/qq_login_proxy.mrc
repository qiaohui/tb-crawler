check process qq_login_proxy with pidfile /var/run/qq_login_proxy.pid
  start program = "cd /space/script; /usr/local/bin/qq_login_proxy.py 8025 --verbose debug --use_logfile --dbhost 192.168.33.161 --db guangbi --dbuser guangbi --dbpasswd guangbi --webdebug --qqhost test.qq.com --pidfile /var/run/qq_login_proxy.pid --daemon"
  stop program="kill `cat /var/run/qq_login_proxy.pid`"
  group guang
