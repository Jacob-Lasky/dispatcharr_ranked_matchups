"""Subset of Dispatcharr 0.32.0 apps/m3u/models.py (see apps/__init__.py)."""

from django.db import models


class M3UAccount(models.Model):
    name = models.CharField(max_length=255, unique=True)
    max_streams = models.PositiveIntegerField(default=0)
    is_active = models.BooleanField(default=True)
