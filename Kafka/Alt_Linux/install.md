## установка пакетов 
`sudo apt-get update`

## docker 

#### проверить доступные версии docker-compose
`apt-cache search docker-compose`

#### версию docker-compose-v2 более старую но написанную на Go ставим чтобы избежать проблем совместимости между docker-compose python библиотек
#### вызывать как docker compose (без _-_)
`sudo apt-get install docker-engine docker-compose-v2` 
`sudo systemctl enable --now docker`

## prereq for portainer
```bash
sudo mkdir -p /opt/portainer
sudo chown adm_soloduhin /opt/portainer
sudo chmod g+r /opt/portainer
```
## nginx
`sudo apt-get install -y nginx apache2-htpasswd`

## nginx ssl+basic_auth
```bash
sudo mkdir -p /etc/nginx/htpasswd
sudo mkdir -p /etc/nginx/ssl/
sudo chmod 700 /etc/nginx/ssl
sudo find /etc/nginx/ssl -name "*.key" -exec sudo chmod 400 {} \;
sudo find /etc/nginx/ssl -name "*.cer" -exec sudo chmod 644 {} \;
```

## создание файла и пользователя админ для kafka-ui-wr
```bash
sudo htpasswd -c /etc/nginx/htpasswd/kafka-ui-wr.htpasswd admin
sudo chgrp _nginx /etc/nginx/htpasswd/kafka-ui-wr.htpasswd
```
## создание файла и пользователя админ для kafka-ui-read-only
``` bash
sudo htpasswd -c /etc/nginx/htpasswd/kafka-ui-read-only.htpasswd admin
sudo chgrp _nginx /etc/nginx/htpasswd/kafka-ui-read-only.htpasswd
```

## проверка доступа
```bash
sudo -u _nginx cat /etc/nginx/htpasswd/kafka-ui-wr.htpasswd
sudo -u _nginx cat /etc/nginx/htpasswd/kafka-ui-read-only.htpasswd
```
## если есть проблемы с доступом
``` bash
sudo chgrp -R _nginx /etc/nginx/htpasswd
sudo chmod 750 /etc/nginx/htpasswd
```
## добавление пользователя
```bash
sudo htpasswd -b -m /etc/nginx/htpasswd/kafka-ui-wr.htpasswd user password
```

## тест авторизации
```bash
curl -Ik -u user:password https://kfk-tst-al-adm01/kafka-ui/
```
## nginx config 
```bash
sudo rm -f /etc/nginx/sites-enabled.d/default
sudo touch /etc/nginx/sites-available.d/01.443.conf
sudo ln -s /etc/nginx/sites-available.d/01.443.conf /etc/nginx/sites-enabled.d/
```
