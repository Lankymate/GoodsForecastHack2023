# GoodsForecastHack2023
Мое топ-10 (всего в направлении ~30 команд) решение [хакатона](https://www.zavodit.ru/ru/calendar/event/38) от некой конторы GoodsForecast, по заявлениям сотрудничающей (как консалтинг?) с Дикси и Перекрестком.

## Задача
Аналитическая команда организаторов разработала систему на эвристиках, которая сигнализирует сотрудникам магазина, когда какого-то товара потенциально не осталось на полке, в след за чем сотрудники проверяют факт наличия, помечая сигнал или **верным** (товара действительно нет), или **неверным** (товар на полке все-таки есть). Задача хакатона - научиться предсказывать результат этой проверки.

Мысли:
* Первое, что как обычно приходит в голову: зачем здесь вообще ML? 

При адекватных операционных процессах в БД кладется информация о *поставках* и *остатках товара на складе*, *вместительности полки*, *максимальном объеме товара на этой полке* и *продажах*, немного поколдовав с которыми можно в любой момент времени однозначно сказать, есть ли на полке товар.

К сожалению, адекватных операционных процессов в ритейле не существуют, поэтому что-то из описанного почти наверняка будет сломано/не реализовано, а чинить/реализовывать такие вещи чаще всего никому не нужно. Как правило витрины продаж всегда в надлежащем состоянии (все-таки из них бизнес считает деньги), но можно уверенно сказать, что в данных о поставках и остатках пропусков будет не менее 50%, информацию о вместительности полок и должном объеме товара на этой полке можно и вовсе считать мифической.

Отсюда, скорее всего, и появляется тяга решить задачу с помощью бесконечно хайпового машинного обучения.

* Следующий возникающий в голове вопрос: какая адекватная метрика сможет оценить эти сигналы?

Здесь проблема схожа с оценкой онлайн-метрик при решении задачи антифрода: если я правильно понял объяснения организаторов при старте хакатона, в выборке не было настоящих ложноотрицательных сигналов. Пусть метка *True* - товара на полке не осталось. Тогда матрица ошибок прогнозов/фактов алгоритма, предсказывающего отсутствие:

|          | Predicted True | Predicted False |
|----------|----------------|-----------------|
| True     | TP  | ?  |
| False    | FP | TN   |

Поскольку сотрудники проверяют полки только когда появляется сигнал, нет возможности определить, является ли прогноз False истинно верным, отсюда невозможность отличить **False Negative** (FN) метки от **True Negative** (TN) и посчитать recall классификатора. Но это уже бизнесовые рассуждения, все-таки антифрод - часто возникающая задача, так что подходов к ее решению должно быть немало, просто я с ними незнаком.

В данном соревновании организаторы, судя по всему, хотели решить задачу отделения **True Positive** (TP) от **False Positive** (FP) меток их собственного эвристического подхода, сравниваться предлагали по классическому **ROC-AUC**. Еще одно замечание: на самом деле всего треков было два, с двумя сообственными раздельными лидербордами и соответсвующими призовыми. В финал (серию питчингов) проходили топ-5 команд по метрике на итоговом ЛБ из каждого направления.

## Хакатон
Решить задачу предлаглось за 2 дня: c вечера пятницы до полудня воскресенья. В хакатоне должны были участвовать команды от двух до пяти человек, я зарегистрировал вторым участником девушку (никак не связанную с дата синсом) чтобы соответствовать требованиям орагниазторов, по итогу решал все в одиночку :)

С самого старта стало понятно, что организаторы не самые опытные в плане организации соревнований, их попытки что-то объяснить по ходу сорвенования оставляли или в полном непонимании, или вызывали только больше вопросов. У меня за два дня это породило в голове кучу мемов, которые потом еще и нашлось время реализовать, дальше по тексту будут представлены именно они.

### Лидерборд
На первом же созвоне вскрылось, что публичный лидерборд будет обновляться на трех чекпоинтах путем отправки через гугл-форму csv-шки с ключами и соответствующими прогнозными вероятностями. Смешная деталь состояла в том, что все выбранные для оценки ключи были частью трейна, а значит никакой реальной информации о своем положении на ЛБ возможности получить не было, поскольку ничто не мешало участникам просто отправить в качестве прогнозов уже проставленные метки из трейна или обучиться на трейне, сделав на нем же итоговый прогноз.

Более того, метрики на приватном ЛБ все смогли бы увидеть в 11 часов воскресенья, после чего у всех участников было бы еще 3 часа, чтобы подготовить НОВЫЙ ИТОГОВЫЙ САБМИТ в надежде подняться на приватном ЛБ...
![image](https://github.com/Lankymate/GoodsForecastHack2023/assets/91146419/692843af-70a5-414e-a3c1-7d1ac5371647)

### Выгрузка данных
Следующим локальным мемом стала выгрузка данных. Орги пообещали трейн из пары месяцев с историей проверок различных товаров в ограниченном пуле магазинов в 100 тысяч строк и 60 анонимизированных фичей, в дополнение к которому шли бы необработанные истории поминутных продаж и дневных стоков (по 50-100 млн. строчек), которые участники могли бы использовать по собственному желанию для генерации новых признаков.

Как оказалось, для этого предоставлялся доступ к СУБД, из которой SQL-запросами можно было бы выгрузить желаемые таблички, чему я несказанно обрадовался (как оказалось, слишком рано, но об этом позже). Хоть это и не был привычный PySpark, SQL по моим ожиданиям хоть сколько-то адекватно должен был помочь нагенерить признаки для временных рядов через оконки. Заниматься таким в богомерзком pandas-е на данных размера 10**6 не было никакого желания.

Проблемы, однако, начались уже здесь. Код для подключения к БД, по заверениям организаторов бесперебойно работавший у них даже на некорпоративных компьютерах, отказывался заводиться без установки некоторых microsoft-овских дров, которые, как оказалось, были необходимы для работы с развернутой в БД MICROSOFT SQL SERVER образца 2012 года (oh boy).
![image](https://github.com/Lankymate/GoodsForecastHack2023/assets/91146419/437db9cf-d36c-47e2-be82-3e804f1c3115)

Проблема, к счастью, решалась интенсивным гуглением, но вот у большинства участников по этому поводу началось лютое-бешеное горение, выражавшееся в бомбежке чатика с просьбами скинуть готовые CSV или ссылку на диск с ними (те самые труъ Дата Саентисты). Породив в собственной голове достаточное количество мемов на эту тему, я эзакрыл чатик и больше не открывал его до конца соревнования. Вот и они:
![image](https://github.com/Lankymate/GoodsForecastHack2023/assets/91146419/08e3b6d2-594d-4092-b3c2-4549a48fa207)
![image](https://github.com/Lankymate/GoodsForecastHack2023/assets/91146419/e61e78da-bead-4e65-b83b-b399540c0c69)
![image](https://github.com/Lankymate/GoodsForecastHack2023/assets/91146419/5b8618af-cdf8-40c2-95d5-8756ad6f5809)

### Решение
Поскольку в своей команде я был единственным участником, планов обучать огромный зоопарк моделей не было, скорее пытался накинуть полезных фичей, сделать адекватную кросс-валидацию и обучить робастный бустинг. В первый вечер я занимался:
1. EDA, не найдя особых инсайтов кроме дисбаланса классов 1:5 в пользу 0, постепенно подозрительно уменьшающегося к последним двум неделям (они же две недели из паблик ЛБ)
2. Отсевом бесполезных, пустых или слишком коррелирующих признаков
3. Попыткой понять, какие из заявленных признаков похожи на категориальные, поиском категорий и подкатегорий товаров для feature engineering-а
4. Разбиением для time-series валидации
5. Оценкой на полученных фолдах бустинга для собственного бейзлайна

По окончании описанного подготовил файлик с прогнозами для ЛБ и лег спать с мыслями о том, как буду накидывать весь следующий день в SQL-е миллион типичных для временных рядов лаговых фичей на стоках и продажах.
Каково было мое разочарование, когда написав на следующий день первую оконку и послав запрос, я обнаружил, что в поставленной оргами версии tSQL-я для оконных функций поддерживается ограничение окна только с помощью `ROWS BETWEEN`, а не `RANGE BETWEEN`...
![image](https://github.com/Lankymate/GoodsForecastHack2023/assets/91146419/943d5859-d586-4f43-9caa-ce88d6977b04)

Потратив половину дня на безуспешные попытки обойти это бессмысленное ограничение, еще какое-то время на поиск решения с помощью pandas-а (спойлер: адекватной альтернативы в нем нет, pandas при любых попытках пытается сломать партицию и ограничения в ней, особенно когда в датах есть пропуски), а еще на гнев, торг, отрицание и желание бросить хакатон на половине, прикинул (а потом переприкинул и еще раз переприкинул...), сколько в среднем строчек приходится на дневной лаг в разных партициях, например ТОВАР/МАГАЗИН, ТОВАР, МАГАЗИН, попытался реализовать задуманные фичи с помощью `ROWS BETWEEN`, все потуги можно наблюдать в файле SQL.py.

Одна за одной большинство из них имели околонулевую корреляцию с таргетом и не давали никакого ощутимого прироста в метрике. К сожалению, на выяснение этого факта, суммирующегося в одной строчке, я потратил остаток времени для решения. Загрузил к одиннадцатичасовому чекпоинту прогнозы на полу-приватном ЛБ и решил от злости/усталости не пытаться улучшить результат в оставшиеся 3 часа, а оставить все как есть, вместо этого потратив их с пользой - клепая собранные в голове мемчики.

Итог - 10е место и желание никогда больше не участвовать в хакатонах от неизвестных сомнительных организаций, тетрадка с решением: [ноутбук](https://nbviewer.org/github/Lankymate/GoodsForecastHack2023/blob/main/solutioin.ipynb).
![image](https://github.com/Lankymate/GoodsForecastHack2023/assets/91146419/e3ebfb3e-e4a8-4763-89c1-c4c9eeeba989)


## Мораль
1. Плохой организатор не сделает нормальное соревнование. После всех приколов с лб и датасетами еще и оказалось, что конторка к концу трейна выкатила в прод модель с новыми эвристиками, которая в разы чаще стала прогнозировать отсутствие товара (отсюда и перемена в дисбалансе классов). Из всех доступных для обучения данных 90% были из прогнозов старой модели, а только 10% и, естественно, весь приватный тест из прогнозов новой. Так что, скорее всего, можно было с легкостью откинуть весь старый трейн и обучаться только на этих 10%. Возможно так и сделали те, кто нашел это при EDA и попал в топ ЛБ. Меня питч-сессии после всех разочарований не слишком интересовали, так что не могу сказать, что там у топовых решений.
2. Я настолько привык на работе к удобству PySpark-а, в котором возможно легко и удобно, а также с максимально понятным и читаемым синтаксисом проделать любую мыслимую (а также первоначально немыслимую) махинацию с достаточно большими табличными данными, что все остальные инструменты кажутся по сравению с ним какими-то отсталыми. Кажется придется поднимать собственный Spark и загружать в него данные из соревнования, если хочется и дальше участвовать в табличных. Правда нужно заресерчить, можно ли сделать это эффективно и без боли на домашнем ПК или лучше, но может хоть не слишком дорого, поднять для этих целей отдельный сервер.
3. Участие в соло имеет и плюсы, и минусы. Из плюсов: от начала до конца пилишь собственный код и свое решение, нет необходимости кидаться кодом и новыми фичами в остальных участников команды, тестируешь свою способность как Дата Саентиста реализовать готовый пайплайн (от EDA до обучения моделей). Из минусов: очевидно успеваешь не так много, как смог бы с командой, а еще не имеешь альтернативной точки зрения на свои идеи. Во время соревнования был момент, когда я пару часов потратил на гарантированно нерабочую реализацию фичи, поняв это только после ее реализации, хотя на деле все было довольно очевидно на этапе формирования идеи для нее, просто не хватило чужого незамыленного взгляда, который бы смог сразу сказать, что направление нерабочее.
4. На соревновании длиной в 2 дня съедаются абсолютно все силы. Хоть я и не упарывался в написание кода ночью, провести 14 часов субботы на стуле, глядя в монитор, было не слишком приятно. Особенно неприятно бросать, зная, что столько времени уже было затрачено на решение. Даже несмотря на то, что такие мысли летали в моей голове всю субботу из-за косяков организации, какое-то решение я все же решил произвести на свет. В целом, наверное, было бы интересно попробовать порешать более длительное соревнование, осталось найти подходящую интересную тему.
