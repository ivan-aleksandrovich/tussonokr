# Используем за основу контейнера Ubuntu 14.04 LTS
FROM ubuntu:14.04
# Переключаем Ubuntu в неинтерактивный режим — чтобы избежать лишних запросов
ENV DEBIAN_FRONTEND noninteractive
# Добавляем необходимые репозитарии и устанавливаем пакеты
RUN apt-get update

# Добавляем описание виртуального хоста

RUN apt-get install ansible -y


RUN sed -e 's/#host_key_checking = False/host_key_checking = False/' -i  /etc/ansible/ansible.cfg
RUN sed -e 's/#log_path = /log_path = /' -i  /etc/ansible/ansible.cfg

RUN mkdir -p /etc/ansible/group_vars

RUN cd /etc/ansible/group_vars

RUN touch cashbox
RUN echo "---" > cashbox
RUN echo -e "ansible_ssh_user: root" > cashbox
RUN echo -e "ansible_ssh_pass: pos" > cashbox
RUN echo -e "ansible_ssh_pass: 111" > cashbox

touch ~/ansible/playbooks/update_ubuntu.yml

VOLUME ["/etc/ansible","root/ansible/playbooks"]

VOLUME /etc/ansible

VOLUME  ~/ansible/playbooks
