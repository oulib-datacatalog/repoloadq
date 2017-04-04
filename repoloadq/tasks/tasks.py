from celery.task import task
from celery import Celery
from celery import signature

import logging

logging.basicConfig(level=logging.INFO)

s3_bucket = "ul-bagit"
s3_source = "source"
s3_derivative = "derivative"

#celery = Celery()
#celery.config_from_object("celeryconfig")


@task()
def loadbook(bag, mmsid, outformat="JPEG", filter="ANTIALIAS", scale=0.4, crop=None, collection='islandora:bookCollection'):
    """
    Generate derivative of Bag and load into Islandora.
    
    args:
      bag: name of bag to load
      mmsid: MMS ID is needed to obtain MARC XML
      outformat - string representation of image format - default is "JPEG". 
                  Available Formats: http://pillow.readthedocs.io/en/3.4.x/handbook/image-file-formats.html
      scale - percentage to scale by represented as a decimal
      filter - string representing filter to apply to resized image - default is "ANTIALIAS"
      crop - list of coordinates to crop from - i.e. [10, 10, 200, 200]
      collection: Name of Islandora collection to ingest to. Default is: islandora:bookCollection  
    """

    # Generate derivatives and store in s3 an local
    deriv_gen = signature("imageq.tasks.tasks.derivative_generation",
                          kwargs={'bags': bag,
                                  's3_bucket': s3_bucket,
                                  's3_source': s3_source,
                                  's3_destination': s3_derivative,
                                  'outformat': outformat,
                                  'filter': filter,
                                  'scale': scale,
                                  'crop': crop
                                  })
    # generate recipe files and process derivatives into bags
    process_derivs = signature("recipewriterq.tasks.tasks.process_derivative", kwargs={'mmsid': mmsid})
    # load into islandora
    ingest_recipe = signature("islandoraq.tasks.tasks.ingest_recipe", kwargs={'collection': collection})

    chain = (deriv_gen | process_derivs | ingest_recipe)
    result = chain()
    return result.get()
