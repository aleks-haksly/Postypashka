Теперь вас ждёт лабораторная работа. На этот раз у вас не будет на руках датасета, поэтому собрать вы его должны сами.
Ваша задача будет собрать датасет, проанализировать рынок конкретной отрасли, и сделать вывод.

Для этого вам предоставлен реестр всех предпринимателей из ФНС. 
Вам следует его подготовить к работе (вас интересуют юридические лица, средние и малые предприятия, в видах деятельности указывайте 41.20 Строительство жилых и нежилых зданий).

Определите насколько перспективна отрасль строительства с точки зрения роста чистой прибыли и [EBITDA](https://xn--80aapampemcchfmo7a3c9ehj.xn--p1ai/news/chto-takoe-ebitda/#:~:text=EBITDA%2520%253D%2520%25D0%259F%25D1%2587%2520%252B%2520%25D0%259D%25D0%259F%2520%252B%2520%25D0%259F%25D1%2580,%25D0%25A4%25D0%25BE%25D1%2580%25D0%25BC%25D1%2583%25D0%25BB%25D0%25B0%2520%25D0%25BF%25D0%25BE%25D0%25B7%25D0%25B2%25D0%25BE%25D) всех компаний по годам.

В данной таблице не будут выделены данные, на которых можно строить метрики, поэтому вам потребуется с [этой](https://bo.nalog.ru/) странички спарсить все нужные данные. Для парсинга советуется использовать [requests.Session()](https://requests.readthedocs.io/en/latest/user/advanced/), но конечно и любые другие технологии, которые найдёте на просторах интернета. Пример работы с запросами:
```python
session = requests.Session()
res = session.get(f"https://bo.nalog.ru/advanced-search/organizations/search?query={здесь указывается ИНН}&page=0").json()
org_id = res["content"][0]["id"]
```
Также вам предстоит построить графики:
1. x=процент роста чистой прибыли компаний относительно N-летней давности, y=Количество компаний с таким процентом)
* x — целое число,
* y — подбирайте на своё усмотрение, где можете импровизировать.

Также требуется написать вывод после увиденных графиков.

Затем ознакомьтесь с `geopandas` и нанесите топ-500 компаний по выручке на карту по месту регистрации.

#### Проблемы с которыми вы можете столкнуться:
* Ваш компьютер не справился с вычислениями, тогда можно уменьшить датасет, рандомно удалив часть компаний. (Иначе говоря просто взяв рандомную выборку).
* Вашу сессию может прервать сайт, в таком случае используйте объект Session().
* Графики у вас может не отобразить нормально, тогда можете начать работать, к примеру, с гистограммами, где каждая отвечает за процент и на 10 процентов.

#### Формат сдачи работы:
Создайте репозиторий на GitHub.com, сделайте ветку dev, добавляйте туда бэкап. Добавляйте изменения и делайте pull request ветки dev в main. Для тех кто не работает с git — можете [углубиться](https://learngitbranching.js.org/?locale=ru_RU) в систему git.

Также обращаю внимание на то, что добавлен новый файл в архив d30 - PandasMatplotlibTasks. Эти задачи рекомендуется выполнить до лабораторной.
