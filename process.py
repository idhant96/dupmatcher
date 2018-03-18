from fuzzywuzzy import process
import multiprocessing
import re
from copy import deepcopy
import sys


class Big(object):
    col_scores = {}
    scores = {}
    col_names = {}

    @staticmethod
    def chunker(df, num_processes):
        chunk_size = int(df.shape[0] / num_processes)
        chunks = [df.ix[df.index[i:i + chunk_size]] for i in range(0, df.shape[0], chunk_size)]
        objects = []
        for i in range(len(chunks)):
            for j in range(len(chunks)):
                if i <= j:
                    objects.append((chunks[i], chunks[j]))
        return objects

    @classmethod
    def process_dataframe(cls, df, col_scores, col_names):
        cls.col_scores = col_scores
        cls.col_names = col_names
        result = []
        df = df.fillna('')
        num_processes = multiprocessing.cpu_count() - 1
        pool = multiprocessing.Pool(processes=num_processes)
        if 'gender' in col_scores.keys():
            male = df.loc[df['gender'] == 'male']
            female = df.loc[df['gender'] == 'female']
            male_chunks = cls.chunker(male, num_processes)
            result.append(pool.starmap(cls.dup, male_chunks))
            # print('male done')
            female_chunks = cls.chunker(female, num_processes)
            result.append(pool.starmap(cls.dup, female_chunks))
            # print('female done')
        else:
            chunks = cls.chunker(df, num_processes)
            result.append(pool.starmap(cls.dup, chunks))

        return result

    @staticmethod
    def name_matcher(name1, name2, total):
        n = []
        n.append(name2.upper())
        match = process.extract(name1.upper(), n, limit=3)[0][1]
        score = (match/100)*total
        return score

    @classmethod
    def absent_fields(cls, obj1, obj2):
        ab = 0
        for col in cls.col_names.keys():
            if col in cls.col_scores.keys():
                if obj1[cls.col_names[col]] == obj2[cls.col_names[col]] == '':
                    ab = ab + cls.col_scores[col]
        return ab

    @staticmethod
    def checker(field1, field2, score, ad):
        if field1 == field2 == '':
            return score
        if field1 == field2:
            score = score + ad
            return score
        return score

    @classmethod
    def field_checker(cls, obj1, obj2, score):
        ab = cls.absent_fields(obj1, obj2)
        scores = deepcopy(cls.col_scores)
        if ab > 0:
            ab = 100 - ab
            for col in scores.keys():
                if col == 'name':
                    continue
                scores[col] = 100 * (scores[col] / ab)
        score = 0
        for field in cls.col_names.keys():
            if field == 'name':
                continue
            if field in cls.col_scores.keys():
                score = cls.checker(obj1[cls.col_names[field]], obj2[cls.col_names[field]], score=score, ad=scores[field])
        return score

    @classmethod
    def dup(cls, chunk1, chunk2):
        name_score = cls.col_scores['name']
        lower = 0.4*name_score
        upper = 0.95*name_score
        cols = cls.col_names
        p = {}
        for pos1, obj1 in chunk1.iterrows():
            matched = []
            name1 = obj1[cols['name']]
            for pos2, obj2 in chunk2.iterrows():
                if obj1[cols['id']] != obj2[cols['id']]:
                    name2 = obj2[cols['name']]
                    n1 = re.findall(r'DR(\s|\.)(.+)', obj1[cols['name']].upper())
                    n2 = re.findall(r'DR(\s|\.)(.+)', obj2[cols['name']].upper())
                    if n1:
                        name1 = n1[0][1]
                    if n2:
                        name2 = n2[0][1]
                    name = cls.name_matcher(name1, name2, name_score)
                    if lower < name < upper:
                        if cols['mobile'] and cols['email'] is not None:
                            if obj1[cols['mobile']] == obj2[cols['mobile']] or obj2[cols['email']] == obj1[cols['email']]:
                                if obj1[cols['mobile']] == '' or obj1[cols['email']] == '':
                                    continue
                                score = cls.field_checker(obj1, obj2, name)
                                if score+name >= 70:
                                    matched.append(str(obj2[cols['id']]) + '-'+ str(round(score+name, 2)))
                                    chunk2 =chunk2.query('id != {}'.format(obj2.id))
                    elif name >= upper:
                        score = cls.field_checker(obj1, obj2, name)
                        if score+name >= 70:
                            matched.append(str(obj2[cols['id']]) + '-'+ str(round(score+name, 2)))
                            chunk2 = chunk2.query('id != {}'.format(obj2.id))
            if matched:
                p[pos1] = matched
        return p


