#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: ai ts=4 sts=4 et sw=4

"""
Project: syn_data
App: apps.accounts.management.commands
FILE: de_bundle_eob
Created: 3/20/18 10:57 AM

Created by: '@ekivemark'

Get each eob bundle and break out individual eobs to separate collection

"""
from django.core.management.base import BaseCommand
from django.conf import settings

import logging
import requests


from pymongo import MongoClient

__author__ = "Mark Scrimshire @ekivemark"

logger = logging.getLogger('syn_data.%s' % __name__)

def de_bundle_eob(collection='bb_fhir'):
    """

    :param collection:
    :return: True

    Split eob bundles to individual EOBs
    """

    client = MongoClient()
    db = client[collection]

    patients = db.patients
    coverage = db.coverage
    eob = db.eob
    erecords = db.erecords

    cursor = eob.find(modifiers={"$snapshot": True})

    index = 0
    counter = cursor.count()

    while index != counter:
        if bundle['link'][0]['url'].split('&')[1] <= 'patient=19990000010000':
            print("skipping %s" % bundle['link'][0]['url'].split('&')[1])
        else:
            bundle = cursor[index]

            if 'entry' in bundle:
                bundle_size = len(bundle['entry'])
            else:
                bundle_size = 0

            print('%s has %s EOBs' % (bundle['link'][0]['url'].split('&')[1],
                                      bundle_size))

            for n in range(1, bundle_size):
                b_n = bundle['entry'][n-1]
                logger.debug("inserting EOB:%s" % n-1)
                b_id = erecords.insert_one(b_n).inserted_id

        index += 1

    cursor.close()

    return True


class Command(BaseCommand):

    help = ("split EOB bundles into individual EOB Records")

    def add_arguments(self, parser):
        parser.add_argument('--confirm',
                            help="confirm action=YES")

        parser.add_argument('--targetcollection',
                            help="Target MongoDB collection [bb_fhir]")


    def handle(self, *app_labels, **options):

        if options['confirm']:
            if options['confirm'] != "YES":
                return False
            else:
                pass

        if options['targetcollection']:
            collection = options['targetcollection']
        else:
            collection = "bb_fhir"

        e = de_bundle_eob(collection)
