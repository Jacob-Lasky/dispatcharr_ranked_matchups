"""Original poster-provider badges, including shared college marks and misses."""
from PIL import Image
from dispatcharr_ranked_matchups import logos


def test_transparent_team_badge_uses_existing_provider_cache(tmp_path,monkeypatch):
    calls=[]
    monkeypatch.setattr(logos,'_http_get_json',lambda url,endpoint,**kw:
        {'teams':[{'strTeam':'NC State','strSport':'American Football',
                   'strLeague':'NCAA Division 1','strBadge':'https://example.test/badge.png'}]})
    def download(url,path,**kw):
        calls.append(url)
        Image.new('RGBA',(64,64),(255,0,0,128)).save(path)
        return 200,None
    monkeypatch.setattr(logos,'download_to_file',download)
    path,reason=logos.cached_team_badge('NC State','CBB',str(tmp_path))
    assert path and reason=='downloaded badge'
    assert Image.open(path).getpixel((0,0))[3]==128
    monkeypatch.setattr(logos,'_http_get_json',lambda *a,**kw: (_ for _ in ()).throw(AssertionError('cache issued HTTP')))
    assert logos.cached_team_badge('NC State','CBB',str(tmp_path))[0]==path
    assert len(calls)==1


def test_wrong_league_and_ambiguous_team_fall_back(tmp_path,monkeypatch):
    rows=[{'strSport':'Ice Hockey','strLeague':'NHL','strBadge':'https://example.test/a.png'}]
    monkeypatch.setattr(logos,'_http_get_json',lambda *a,**kw:{'teams':rows})
    assert logos.cached_team_badge('NC State','CBB',str(tmp_path))[0] is None
    rows.extend(rows.copy())
    assert logos.cached_team_badge('Ambiguous','NHL',str(tmp_path))[0] is None


def test_transient_lookup_is_not_negative_cached(tmp_path,monkeypatch):
    calls=[]
    monkeypatch.setattr(logos,'_http_get_json',lambda *a,**kw:calls.append(1))
    for _ in range(2):
        assert logos.cached_team_badge('Nashville Predators','NHL',str(tmp_path)) == (None,'team lookup unavailable')
    assert len(calls)==2


def test_missing_badge_is_negative_cached(tmp_path,monkeypatch):
    calls=[]
    def lookup(*a,**kw):calls.append(1);return {'teams':None}
    monkeypatch.setattr(logos,'_http_get_json',lookup)
    for _ in range(2):
        path,reason=logos.cached_team_badge('Missing Emblem','NHL',str(tmp_path))
        assert path is None and 'no unambiguous' in reason
    assert len(calls)==1
