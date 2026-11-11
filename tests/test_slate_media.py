"""Real render/loop contract: media clocks, headers, artwork, cache reuse."""
import json
import shutil
import subprocess
import time
from datetime import datetime,timedelta,timezone

import pytest
from PIL import Image,ImageDraw
from dispatcharr_ranked_matchups import slate,logos


def test_cached_crests_and_independent_missing_logo(tmp_path):
    badge=Image.new('RGBA',(500,500))
    ImageDraw.Draw(badge).rectangle((50,50,450,450),fill='red')
    path=tmp_path/'crest.png';badge.save(path)
    card=tmp_path/'card.png'
    slate._draw_card({'home':'NC State','away':'Wake Forest','league':'NCAA Basketball',
        'status':'Live at 7:00 PM ET','logos':{'away':str(path),'home':None}},card)
    with Image.open(card) as image:
        assert image.size==(1920,1080)
        away=image.crop((400,310,670,590));home=image.crop((1250,310,1520,590))
        assert sum(r>200 and g<60 for r,g,b in away.getdata())>1000
        assert sum(r>200 and g>200 and b>200 for r,g,b in home.getdata())>1000
        # Even a square crest's corners stay inside the padded badge circle.
        red=[(x,y) for y in range(285,616) for x in range(370,701)
             if image.getpixel((x,y))==(255,0,0)]
        assert max((x-535)**2+(y-450)**2 for x,y in red) <= 141**2


@pytest.mark.skipif(not shutil.which('ffmpeg'),reason='media integration needs ffmpeg')
def test_loop_clocks_headers_and_cache(tmp_path,monkeypatch,plugin):
    monkeypatch.setattr(slate,'cache_dir',lambda:tmp_path/'slates')
    monkeypatch.setattr(logos,'LOGO_DIR',str(tmp_path))
    monkeypatch.setattr(logos,'cached_team_badge',lambda *a:(None,'test missing badge'))
    now=datetime.now(timezone.utc)
    game={'home':'NC State','away':'Wake Forest','sport_prefix':'CBB',
          'start_time_utc':(now+timedelta(minutes=40)).isoformat(),'extra':{'cbb_id':'w323'}}
    url=slate.render(game,now)
    clip=next((tmp_path/'slates').glob('*.mp4'))
    mtime=clip.stat().st_mtime_ns
    assert slate.render(game,now+timedelta(seconds=1))==url
    assert clip.stat().st_mtime_ns==mtime
    assert slate.render(game,now+timedelta(hours=1))!=url
    capture=tmp_path/'loop.ts'
    with capture.open('wb') as output:
        for packet in slate.packets(clip,time.time()+11):output.write(packet)
    info=json.loads(subprocess.check_output(['ffprobe','-v','error','-show_streams','-of','json',str(capture)]))
    video,audio=info['streams']
    assert video['r_frame_rate']=='5/1'
    assert video['width']==1920 and video['height']==1080
    assert audio['codec_name']=='ac3' and audio['channels']==2 and audio['sample_rate']=='48000'
    trace=subprocess.run(['ffmpeg','-hide_banner','-i',str(capture),'-map','0:v','-c','copy',
        '-bsf:v','trace_headers','-f','null','-'],capture_output=True,text=True,check=True).stderr
    # Every IDR must carry an SPS and PPS, including after the clip wraps.
    import re
    types=re.findall(r'nal_unit_type\s+[01]+ = (\d+)',trace)
    assert types.count('5')>=3
    for i,kind in enumerate(types):
        if kind=='5':assert '7' in types[max(0,i-4):i] and '8' in types[max(0,i-4):i]
