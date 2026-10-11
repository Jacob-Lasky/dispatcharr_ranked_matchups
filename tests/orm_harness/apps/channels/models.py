"""Subset of Dispatcharr 0.32.0 apps/channels/models.py (see apps/__init__.py).

The on_delete rules are the part that matters most: Recording.channel CASCADEs,
which is why apply re-homes recordings before it deletes a channel (#146), and a
harness that got it wrong would pass a test that loses the user's recordings.
"""

from django.db import models
from django.utils import timezone


class ChannelGroup(models.Model):
    name = models.TextField(unique=True, db_index=True)


class Logo(models.Model):
    name = models.CharField(max_length=255)
    url = models.TextField(unique=True)


class Stream(models.Model):
    name = models.CharField(max_length=512, default="Default Stream", db_index=True)
    url = models.URLField(max_length=4096, blank=True, null=True)
    m3u_account = models.ForeignKey(
        "m3u.M3UAccount", on_delete=models.CASCADE, null=True, blank=True,
        related_name="streams",
    )
    tvg_id = models.CharField(max_length=255, blank=True, null=True)
    updated_at = models.DateTimeField(auto_now=True)
    channel_group = models.ForeignKey(
        ChannelGroup, on_delete=models.SET_NULL, null=True, blank=True,
        related_name="streams",
    )
    last_seen = models.DateTimeField(db_index=True, default=timezone.now)
    custom_properties = models.JSONField(default=dict, blank=True, null=True)
    stream_stats = models.JSONField(null=True, blank=True)


class Channel(models.Model):
    channel_number = models.FloatField(db_index=True, null=True, blank=True)
    name = models.CharField(max_length=512)
    logo = models.ForeignKey(
        Logo, on_delete=models.SET_NULL, null=True, blank=True, related_name="channels",
    )
    streams = models.ManyToManyField(
        Stream, blank=True, through="ChannelStream", related_name="channels",
    )
    channel_group = models.ForeignKey(
        ChannelGroup, on_delete=models.SET_NULL, null=True, blank=True,
        related_name="channels",
    )
    tvg_id = models.CharField(max_length=255, blank=True, null=True)
    epg_data = models.ForeignKey(
        "epg.EPGData", on_delete=models.SET_NULL, null=True, blank=True,
        related_name="channels",
    )
    auto_created = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)


class ChannelProfile(models.Model):
    name = models.CharField(max_length=100, unique=True)


class ChannelProfileMembership(models.Model):
    channel_profile = models.ForeignKey(ChannelProfile, on_delete=models.CASCADE)
    channel = models.ForeignKey(Channel, on_delete=models.CASCADE)
    enabled = models.BooleanField(default=True)

    class Meta:
        unique_together = ("channel_profile", "channel")


class ChannelStream(models.Model):
    channel = models.ForeignKey(Channel, on_delete=models.CASCADE)
    stream = models.ForeignKey(Stream, on_delete=models.CASCADE)
    order = models.PositiveIntegerField(default=0)

    class Meta:
        ordering = ["order"]
        constraints = [
            models.UniqueConstraint(fields=["channel", "stream"], name="unique_channel_stream"),
        ]


class Recording(models.Model):
    channel = models.ForeignKey(Channel, on_delete=models.CASCADE, related_name="recordings")
    start_time = models.DateTimeField()
    end_time = models.DateTimeField()
    task_id = models.CharField(max_length=255, null=True, blank=True)
    custom_properties = models.JSONField(default=dict, blank=True, null=True)
