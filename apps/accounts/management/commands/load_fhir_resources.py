#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: ai ts=4 sts=4 et sw=4

"""
Project: syn_data
App: apps.
FILE: load_fhir_resources
Created: 3/19/18 8:27 PM

Created by: '@ekivemark'
"""
from django.core.management.base import BaseCommand
from django.conf import settings

import logging
import requests
import urllib3


from pymongo import MongoClient

__author__ = "Mark Scrimshire @ekivemark"

logger = logging.getLogger('syn_data.%s' % __name__)

def load_fhir_resources(collection='bb_fhir',
                        resources=['Patient', 'Coverage', 'ExplanationOfBenefit'],
                        start_id='19990000000001',
                        end_id='19990000000100'):
    """

    :param collection:
    :param start_id:
    :param end_id:
    :return: True

    Load synthetic data in to mongodb
    """
    urllib3.disable_warnings()

    client = MongoClient()
    db = client[collection]

    patients = db.patients
    coverage = db.coverage
    eob = db.eob
    erecords = db.erecords

    start_id_n = int(start_id)

    end_id_n = int(end_id)

    for n in range(start_id_n, end_id_n):

        if 'Patient' in resources:
            patient_url = settings.FHIR_SERVER + settings.FHIR_PATH + '/Patient/' + str(n) + '/?_format=' + settings.FHIR_FORMAT
            r_p = requests.get(patient_url,
                               cert=settings.FHIR_CERT,
                               verify=settings.FHIR_VERIFY)

            if r_p.status_code == 200:
                patient_info = r_p.json()
                p_id = patients.insert_one(patient_info).inserted_id
                print('Found. Saving Patient info "%s"' % str(n))
            else:
                logger.debug('NOT Found. "%s"' % str(n))

        if 'Coverage' in resources:
            coverage_url = settings.FHIR_SERVER + settings.FHIR_PATH + '/Coverage/?beneficiary=Patient/' + str(n) + '&_format=' + settings.FHIR_FORMAT
            r_c = requests.get(coverage_url,
                               cert=settings.FHIR_CERT,
                               verify=settings.FHIR_VERIFY)
            if r_c.status_code == 200:
                coverage_info = r_c.json()
                c_id = coverage.insert_one(coverage_info).inserted_id
                print('        Saving Coverage for "%s"' % str(n))

        if 'ExplanationOfBenefit' in resources:
            eob_url = settings.FHIR_SERVER + settings.FHIR_PATH + '/ExplanationOfBenefit/?patient=' + str(n) + '&_format=' + settings.FHIR_FORMAT
            r_e = requests.get(eob_url,
                               cert=settings.FHIR_CERT,
                               verify=settings.FHIR_VERIFY)
            if r_e.status_code == 200:
                eob_info = r_e.json()
                e_id = eob.insert_one(eob_info).inserted_id
                print('        Saving EOB for "%s"' % str(n))

                if 'entry' in eob_info.keys():
                    bundle_size = len(eob_info['entry'])
                else:
                    bundle_size = 0

                for n in range(1, bundle_size):
                    b_n = bundle['entry'][n-1]
                    print("inserting EOB:%s" % n-1)
                    b_id = erecords.insert_one(b_n).inserted_id

    return True


class Command(BaseCommand):

    help = ("Load synthetic data to mongodb:fhir")

    def add_arguments(self, parser):
        parser.add_argument('--confirm',
                            help="confirm action=YES")

        parser.add_argument('--targetcollection',
                            help="Target MongoDB collection")

        parser.add_argument('--resources',
                            help='Resources to process: Patient,'
                                 'Coverage,ExplanationOfBenefit')

        parser.add_argument('--start_id',
                            help="Start number")

        parser.add_argument('--end_id',
                            help="End number")

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

        if options['resources']:
            resources = options['resources'].split(',')
        else:
            resources = ['Patient', 'Coverage', 'ExplanationOfBenefit']
        if options['start_id']:
            start_id = options['start_id']
        else:
            start_id = "19990000000001"

        if options['end_id']:
            end_id = options['end_id']
        else:
            end_id = "19990000000100"

        e = load_fhir_resources(collection,
                                resources=resources,
                                start_id=start_id,
                                end_id=end_id)
