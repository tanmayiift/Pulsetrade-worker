from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

_an = SentimentIntensityAnalyzer()

def score_text(t: str) -> float:
    if not t:
        return 0.0
    s = _an.polarity_scores(t)
    return float(s["compound"])

def aggregate_headlines(hls: list[str]) -> float:
    if not hls:
        return 0.0
    xs = [score_text(h) for h in hls[:10]]
    return sum(xs)/max(1,len(xs))


