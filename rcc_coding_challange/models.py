# -*- coding: utf-8 -*-

import json
import sqlite3

# CREATE DB And Table

connection = sqlite3.connect('RCC')
cursor = connection.cursor()
request = open('rcc_coding_challange/data.json')
data = json.load(request)
cursor.execute("CREATE TABLE  IF NOT EXISTS %s (id int(11), name varchar(50) NOT NULL , age int(11) NOT NULL)" % (data['table'][0]['name']))

# INSERT Test Case

tast_data = json.load(open('rcc_coding_challange/tast_case.json'))
for test in tast_data['data']:
    name = "'" + test['name'] + "'"
    fields = ''
    for tab in data['nodes'][0]['transformObject']:
        fields = "(" + (tab['fields'][0]) + ',' + (tab['fields'][1]) + ',' + (tab['fields'][2]) + ")"
    cursor.execute("INSERT INTO %s %s VALUES (%s , %s ,%s)" % (data['table'][0]['name'],fields,int(test['id']),name,int(test['age'])))

query1 = " WITH "
query = " "
next = ''

for edge in data['edges']:
    last = data['edges'][-1]['to']
    query1 += edge['from'] + " as ( " + " "
    for node in data['nodes']:

        if edge['from'] == node['key']:
            puplic_table = ''
            fields = ''

            if node['type'] == 'INPUT':
                for tab in node['transformObject']:
                    fields = (tab['fields'][0])+',' + (tab['fields'][1])+','+(tab['fields'][2])
                    query1 += " SELECT %s FROM %s  " % (fields, tab['tableName']) + ")," + " "
                next = edge['from']

            if node['type'] == 'FILTER':
                for pub in data['table']:
                    puplic_table = pub['name']
                    fields = (pub['fields'][0]) + ',' + (pub['fields'][1]) + ',' + (pub['fields'][2])
                    for tab2 in node['transformObject']:
                        variable_field_name = tab2['variable_field_name']
                        operations = ''
                        value = ''
                        for oper in tab2['operations']:
                            operations = (oper['operator'])
                            value = oper['value']
                        query1 += " SELECT %s FROM %s where %s %s %s" % (fields, next ,variable_field_name,operations,int (value)) + ")," + " "
                        next = edge['from']

            if node['type'] == 'SORT':
                order_by = ''
                for pub in data['table']:
                    puplic_table = pub['name']
                    fields = (pub['fields'][0]) + ',' + (pub['fields'][1]) + ',' + (pub['fields'][2])
                    for tab3 in node['transformObject']:
                        target = tab3['target']
                        order = tab3['order']
                        if tab3 != node['transformObject'][-1] :
                            order_by += tab3['target'] + ' ' + tab3['order'] + ','
                        else:
                            order_by += tab3['target'] + ' ' + tab3['order']
                    query1 += " SELECT %s FROM %s ORDER BY  %s "  % (fields, next ,order_by) + ")," + " "
                    next = edge['from']

            if node['type'] == 'TEXT_TRANSFORMATION':
                column = ''
                transformation = ''
                for pub in data['table']:
                    puplic_table = pub['name']
                    for tab4 in node['transformObject']:
                        column = tab4['column']
                        transformation = tab4['transformation']
                    if column == pub['fields'][0]:
                        fields = transformation + '(' + (pub['fields'][0]) + ')' + ',' + (pub['fields'][1]) + ',' + (pub['fields'][2])
                    if column == pub['fields'][1]:
                        fields = (pub['fields'][0]) + ',' + transformation + '('+ (pub['fields'][1]) + ')' + ',' + (pub['fields'][2])
                    if column == pub['fields'][2]:
                        fields = (pub['fields'][0]) + ',' + (pub['fields'][1]) + ',' + transformation + '(' +(pub['fields'][2]) + ')'
                    query1 += " SELECT %s FROM %s " % (fields, next) + ")," + " "
                    next = edge['from']

        if data['edges'][-1]['to'] == node['key'] and node['type'] == 'OUTPUT':
            for pub in data['table']:
                puplic_table = pub['name']
                for tab5 in node['transformObject']:
                    limit = int (tab5['limit'])
                    offset = int (tab5['offset'])
                    query = " " + data['edges'][-1]['to'] + " as ( " + " "
                    query += " SELECT * FROM %s limit %s offset %s" % (next, limit, offset) + ")"

last_query = query1 + query
cursor.execute(last_query + " SELECT * FROM %s " % (data['edges'][-1]['to']))

test = cursor.fetchall()
print('##################################################################')
print(test)

connection.commit()
connection.close()
