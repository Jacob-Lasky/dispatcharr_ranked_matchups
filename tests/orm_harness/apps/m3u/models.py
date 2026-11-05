"""Subset of Dispatcharr 0.32.0 apps/m3u/models.py (see apps/__init__.py)."""

from django.db import models

CUSTOM_M3U_ACCOUNT_NAME = "custom"

class M3UAccount(models.Model):
    name = models.CharField(max_length=255, unique=True)
    max_streams = models.PositiveIntegerField(default=0)
    is_active = models.BooleanField(default=True)
    updated_at = models.DateTimeField(null=True)
    locked = models.BooleanField(default=False)
    status = models.CharField(max_length=20, default="idle")
    refresh_interval = models.IntegerField(default=0)
