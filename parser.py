import pandas as pd
from yargy import Parser, rule, and_, not_
from yargy.interpretation import fact
from yargy.predicates import gram
from yargy.relations import gnc_relation
from yargy.pipelines import morph_pipeline

#Прочтение данных
df_data = pd.read_csv('test_data.csv')

#Создание правил для поиска требуемых фраз
Name = fact(
    'Name',
    ['first'],
)

Company = fact(
    'Company',
    ['type', 'company_name']
)

FIRST = and_(
    gram('Name'),
    not_(gram('Abbr')),
)

COMPANY_NAME = and_(
    gram('NOUN'),
)

TYPE = morph_pipeline([
    'компания',
])

GREETING = morph_pipeline([
    'здравствуйте',
])

GOODBYE = morph_pipeline([
    'до свидания',
])

gnc = gnc_relation()

NAME = rule(
    FIRST.interpretation(
        Name.first
    ).match(gnc),
).interpretation(
    Name
)

COMPANY = rule(
    TYPE.interpretation(
        Company.type
    ).match(gnc),
    COMPANY_NAME.interpretation(
        Company.company_name
    ).optional().repeatable().match(gnc),
).interpretation(
    Company
)

#Объявление парсеров требуемых словосочетаний
parser_name = Parser(NAME)
parser_company = Parser(COMPANY)
parser_greeting = Parser(GREETING)
parser_goodbye = Parser(GOODBYE)

#Поиск словосочетаний
dlg_ids = []
dlg_greetings = []
dlg_names = []
dlg_company_names = []
dlg_goodbyes = []
dlg_complete = []

for dlg_id in df_data['dlg_id'].unique():
    dlg_ids.append(dlg_id)
    is_greeting = False
    is_goodbye = False
    is_name = False
    is_complete = False
    for line in df_data.loc[(df_data['dlg_id']==dlg_id) & (df_data['role']=='manager')]['text']:
        for match in parser_greeting.findall(line):
            dlg_greetings.append(line)
            is_greeting = True
        for match in parser_name.findall(line):
            if not(is_name):
                dlg_names.append(' '.join([_.value for _ in match.tokens]))
                is_name = True
        for match in parser_company.findall(line):
            dlg_company_names.append(' '.join([_.value for _ in match.tokens]))
        for match in parser_goodbye.findall(line):
            dlg_goodbyes.append(line)
            is_goodbye = True
    if is_greeting and is_goodbye:
        is_complete = True
        dlg_complete.append(is_complete)
    else:
        dlg_complete.append(is_complete)
    if not(is_greeting):
        dlg_greetings.append(None)
    if not(is_goodbye):
        dlg_goodbyes.append(None)   

#Заполнение результата
df_answer = pd.DataFrame(data={'dlg_id': dlg_ids, 'greeting': dlg_greetings, 'name': dlg_names, 'company': dlg_company_names, 'goodbye': dlg_goodbyes, 'is_complete': dlg_complete})

df_answer.to_csv('answer.csv', index=False)