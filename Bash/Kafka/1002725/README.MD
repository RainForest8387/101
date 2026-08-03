Пример:
# сначала посмотреть, что затронет force, ничего не удаляя:
./repartition_topics.sh -p 'drub.*' -f -n
# затем выполнить (спросит ввести yes):
./repartition_topics.sh -p 'drub.*' -f -N 1
# полностью автоматически, без вопросов:
./repartition_topics.sh -p 'drub.*' -f -y -N 1
