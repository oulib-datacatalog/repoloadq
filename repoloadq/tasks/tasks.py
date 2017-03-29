from celery.task import task
from celery import Celery

import logging

logging.basicConfig(level=logging.INFO)

s3_bucket = "ul-bagit"
s3_source = "source"
s3_derivative = "derivative"

celery = Celery()
celery.config_from_object("celeryconfig")


@task()
def loadbook(bag, mmsid, outformat="JPEG", filter="ANTIALIAS", scale=0.4, crop=None, collection='islandora:bookCollection'):
    """
    Generate derivative of Bag and load into Islandora.
    
    args:
      bag: URL pointing to json formatted recipe file
      mmsid:
      outformat - string representation of image format - default is "JPEG". 
                  Available Formats: http://pillow.readthedocs.io/en/3.4.x/handbook/image-file-formats.html
      scale - percentage to scale by represented as a decimal
      filter - string representing filter to apply to resized image - default is "ANTIALIAS"
      crop - list of coordinates to crop from - i.e. [10, 10, 200, 200]
      collection: Name of Islandora collection to ingest to. Default is: islandora:bookCollection  
    """

    # Generate derivatives and store in s3 an local
    taskid = celery.send_task("imageq.tasks.tasks.derivative_generation",
                              kwargs={'bags': bag,
                                      's3_bucket': 'ul-bagit',
                                      's3_source': 'source',
                                      's3_destination': 'derivative',
                                      'outformat': outformat,
                                      'filter': filter,
                                      'scale': scale,
                                      'crop': crop
                                      }).id
    # bag derivative
    celery.send_task("recipewriterq.tasks.tasks.bag_derivatives", (taskid))
    # create recipe file
    celery.send_task("recipewriterq.tasks.tasks.derivative_recipe", (taskid, mmsid))
    # load into islandora
    #celery.send_task("islandoraq.tasks.tasks.ingest_recipe", (url, collection)).get()
    # delete working files