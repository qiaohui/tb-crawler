check process qzone_post with pidfile /var/run/qzone_post.pid
  start program = "cd /space/script; /usr/bin/python /usr/local/bin/post.py --db guangbi --dbuser guangbi --dbpasswd guangbi --fromdb --dbhost 192.168.33.161 --nouse_paperboy --pbcate shuoshuo --pbverbose warning --loop --use_logfile --commitfail --verbose info --timer --statshost 192.168.32.157 --pidfile /var/run/qzone_post.pid --daemon"
  stop program="kill `cat /var/run/qzone_post.pid`"
  group guang
