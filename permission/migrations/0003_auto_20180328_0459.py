# -*- coding: utf-8 -*-
# Generated by Django 1.11.7 on 2018-03-28 04:59
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('permission', '0002_auto_20180328_0456'),
    ]

    operations = [
        migrations.AlterField(
            model_name='permission',
            name='permissions',
            field=models.ManyToManyField(related_name='permissionss', to='auth.Permission', verbose_name='Permissions'),
        ),
    ]
