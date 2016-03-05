from __future__ import absolute_import

from celery import Celery

app = Celery('proj',
             #broker='amqp://guest:guest@54.85.48.21:5672//',
             #backend='amqp',
			 include=['proj.task']
             ) #

# Optional configuration, see the application user guide.
app.conf.update(
	CELERY_RESULT_BACKEND='amqp',
)


if __name__ == '__main__':
    app.start()