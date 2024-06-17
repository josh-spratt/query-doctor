from openai import OpenAI

client = OpenAI()

query_plan = """
XN Hash Join DS_BCAST_INNER  (cost=109.98..3871130276.17 rows=172456 width=132)
  Hash Cond: ("outer".eventid = "inner".eventid)
  ->  XN Merge Join DS_DIST_NONE  (cost=0.00..6285.93 rows=172456 width=97)
        Merge Cond: ("outer".listid = "inner".listid)
        ->  XN Seq Scan on listing  (cost=0.00..1924.97 rows=192497 width=44)
        ->  XN Seq Scan on sales  (cost=0.00..1724.56 rows=172456 width=53)
  ->  XN Hash  (cost=87.98..87.98 rows=8798 width=35)
        ->  XN Seq Scan on event  (cost=0.00..87.98 rows=8798 width=35)
"""

prompt = f"""Given the following Amazon Redshift query plan, interpret the plan for
junior database developers' understanding. Don't make it too verbose, but comment on
each key operator in the plan and describe its function and if there's any indication
to improve it. Specifically focus on JOIN redistribution (DS_BCAST_INNER, DS_DIST_BOTH,
etc. Comment on 'good' vs 'bad' JOINs.) Query plan: {query_plan}"""

completion = client.chat.completions.create(
  model="gpt-3.5-turbo",
  messages=[
    {"role": "user", "content": prompt}
  ]
)

print(completion.choices[0].message)
