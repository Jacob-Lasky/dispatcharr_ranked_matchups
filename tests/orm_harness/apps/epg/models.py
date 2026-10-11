"""Subset of Dispatcharr 0.32.0 apps/epg/models.py (see apps/__init__.py)."""

from django.db import models


class EPGSource(models.Model):
    name = models.CharField(max_length=255, unique=True)
    source_type = models.CharField(max_length=20)
    url = models.URLField(max_length=1000, blank=True, null=True)
    is_active = models.BooleanField(default=True)
    file_path = models.CharField(max_length=1024, blank=True, null=True)
    refresh_interval = models.IntegerField(default=0)
    status = models.CharField(max_length=20, default="idle")


class EPGData(models.Model):
    tvg_id = models.CharField(max_length=255, null=True, blank=True, db_index=True)
    name = models.CharField(max_length=512)
    epg_source = models.ForeignKey(
        EPGSource, on_delete=models.CASCADE, null=True, blank=True, related_name="epgs",
    )

    class Meta:
        unique_together = ("tvg_id", "epg_source")


class ProgramData(models.Model):
    epg = models.ForeignKey(EPGData, on_delete=models.CASCADE, related_name="programs")
    start_time = models.DateTimeField()
    end_time = models.DateTimeField()
    title = models.CharField(max_length=255)
    sub_title = models.TextField(blank=True, null=True)
    description = models.TextField(blank=True, null=True)
    tvg_id = models.CharField(max_length=255, null=True, blank=True)
    custom_properties = models.JSONField(default=dict, blank=True, null=True)
