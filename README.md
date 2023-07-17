https://github.com/googleapis/python-bigquery-sqlalchemy

これの動作確認

ただし、古いバージョンの sqlaclemy 使っているから、使用するのはやめるかも？

---

2023/07/17

SQLalchemy が 2 つ共存できないかもしれない。見た感じ、bigquery のやつと普通のやつ入れるとエラーで動かない
=> 理由は、2.0 と 1.4 どっちも入っちゃうから！これを避けるには、どちらかだけを install しようね！
