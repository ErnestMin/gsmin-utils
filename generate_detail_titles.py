import csv
import re
from itertools import combinations
from pathlib import Path
from typing import List, Tuple

CSV_PATH = Path('요구사항정의서_V2.csv')
OUTPUT_PATH = Path('요구사항정의서_상세요구사항명_2.csv')
PREFERRED_ENCODINGS = ('utf-8-sig', 'utf-8', 'cp949')

SUFFIXES = sorted({
    '으로부터','으로써','으로서','으로는','으로','로부터','로써','로서','로는','로','에게서','에게','께서','부터','까지','에서는','에서','에서의',
    '이라도','라도','이라서','라서','이라면','라면','이라','라','이라고','라고','이라며','라며','이라든지','라든지','이라는','라는','이라든가','라든가',
    '이라야','라야','이라서','이면','라면','이고','이며','와','과','과의','와의','고','하고','하며','하면서','하면서도','하면서는','하면서도','하면서도',
    '하면서도','하며','하면서','하고','하였으며','하였다','하여','하여야','하여야만','하여야함','하여함','함','함을','함과','함께','함에','함으로','하면서',
    '하면서도','면서','면서도','면서는','이고','이면','인가','인가요','인가에','이라든','이라네','이라니','이라','라','마저','조차','밖에','뿐','도록',
    '되어야만','되어야함','되어야','되어','되는','된','될','되며','되도록','되기','되거나','되게','되었으며','되었고','되었으나','되었을','되었다','되었던','되었',
    '하려면','하려고','하기','하기위해','하기위한','하기위해','간의','간','하는','하는지','하는데','하는가','한'
}, key=len, reverse=True)
if '간' in SUFFIXES:
    SUFFIXES.remove('간')
SUFFIX_CHARS = set('은는이가을를과와의도만로에하')

ACTION_KEYWORDS = sorted({
    '수립','구축','구현','구성','제공','지원','관리','운영','연계','연동','통합','모니터링','점검','검증','분석','작성','제출','생성','설정','확인','검토',
    '발굴','추출','정의','개선','개발','유지','정비','전환','도입','적용','준수','확보','추진','강화','통제','교육','훈련','진단','검사','평가',
    '마련','활용','반영','제시','보고','통보','알림','자동화','승인','발송','배포','수집','갱신','업데이트','편성','등록','조회','검색','열람',
    '발급','이행','확장','연결','분류','감시','감독','조정','개편','보완','고도화','최적화','표준화','표준','준용','준비','진행','운용','활성화',
    '비교','공유','정착','제거','전송','배치','변환','이관','배정','재설계','전문화','세분화'
}, key=len, reverse=True)

ACTION_LABELS = {
    '수립': '수립',
    '구축': '구축',
    '구현': '기능 구현',
    '제공': '기능 제공',
    '지원': '지원 체계 구축',
    '관리': '관리체계 수립',
    '운영': '운영 체계 수립',
    '연계': '연계 체계 구축',
    '연동': '연동 기능 제공',
    '통합': '통합 관리체계 구축',
    '모니터링': '모니터링 체계 구축',
    '점검': '점검 체계 수립',
    '검증': '검증 체계 수립',
    '분석': '분석 체계 구축',
    '작성': '작성 체계 수립',
    '제출': '제출 절차 수립',
    '생성': '생성 기능 제공',
    '설정': '설정',
    '확인': '확인 절차 수립',
    '검토': '검토 절차 수립',
    '구성': '구성',
    '발굴': '발굴 체계 구축',
    '추출': '추출 기능 제공',
    '정의': '정의',
    '개선': '개선 방안 수립',
    '개발': '기능 개발',
    '유지': '유지관리 체계 구축',
    '정비': '정비 체계 구축',
    '전환': '전환 방안 수립',
    '도입': '도입',
    '적용': '적용 방안 수립',
    '준수': '준수 체계 수립',
    '확보': '확보 방안 수립',
    '추진': '추진 계획 수립',
    '강화': '강화 방안 수립',
    '통제': '통제 체계 수립',
    '교육': '교육 프로그램 운영',
    '훈련': '훈련 체계 구축',
    '진단': '진단 체계 수립',
    '검사': '검사 체계 구축',
    '평가': '평가 체계 구축',
    '마련': '마련',
    '활용': '활용 체계 구축',
    '반영': '반영 절차 수립',
    '제시': '제시',
    '보고': '보고 체계 수립',
    '통보': '통보 체계 구축',
    '알림': '알림 기능 제공',
    '자동화': '자동화 기능 제공',
    '자동': '자동화 기능 제공',
    '승인': '승인 절차 수립',
    '발송': '발송 기능 제공',
    '배포': '배포 체계 구축',
    '수집': '수집 체계 구축',
    '갱신': '갱신 체계 구축',
    '업데이트': '업데이트 체계 구축',
    '편성': '편성 체계 수립',
    '등록': '등록 기능 제공',
    '조회': '조회 기능 제공',
    '검색': '검색 기능 제공',
    '열람': '열람 기능 제공',
    '발급': '발급 기능 제공',
    '이행': '이행 계획 수립',
    '확장': '확장 방안 수립',
    '연결': '연결 체계 구축',
    '분류': '분류 체계 구축',
    '감시': '감시 체계 구축',
    '감독': '감독 체계 구축',
    '조정': '조정 체계 수립',
    '개편': '개편 방안 수립',
    '보완': '보완 방안 수립',
    '고도화': '고도화 방안 수립',
    '최적화': '최적화 방안 수립',
    '표준화': '표준화 체계 수립',
    '표준': '표준 수립',
    '준용': '준용 기준 마련',
    '준비': '준비 절차 수립',
    '진행': '진행 계획 수립',
    '운용': '운용 체계 수립',
    '활성화': '활성화 방안 수립',
    '비교': '비교 분석 체계 구축',
    '공유': '공유 체계 구축',
    '정착': '정착 방안 수립',
    '제거': '제거 방안 수립',
    '전송': '전송 체계 구축',
    '배치': '배치 계획 수립',
    '변환': '변환 체계 구축',
    '이관': '이관 절차 수립',
    '배정': '배정 체계 구축',
    '재설계': '재설계 방안 수립',
    '전문화': '전문화 방안 수립',
    '세분화': '세분화 방안 수립',
}

ACTION_BONUS = {
    '확보': 5,
    '준수': 4,
    '수립': 4,
    '구축': 4,
    '구현': 4,
    '제공': 4,
    '지원': 3,
    '관리': 2,
    '운영': 3,
    '연계': 3,
    '연동': 3,
    '분석': 3,
    '평가': 3,
    '보고': 3,
    '등록': 3,
    '조회': 3,
    '검색': 3,
    '열람': 3,
    '발급': 3,
    '발송': 3,
    '수집': 3,
    '배포': 3,
    '갱신': 3,
    '정비': 3,
    '전환': 3,
    '도입': 3,
    '적용': 3,
    '개선': 2,
    '강화': 2,
    '확장': 2,
    '공유': 3,
    '전송': 3,
    '이관': 3,
    '재설계': 3,
    '구성': 3,
}

STOPWORDS = set("""
제안 제안사 제안서는 제안요청서 제안요청 사업자 사업 발주기관 기관 감사원 감사원의 감사원과 감사원은 감사원에 감사원과의 및 등의 등을 등에 등으로 등과 등도 각 각종 각각 위해 위하여 위한 통해 통하여 통한 하여 하여야 하고 하며 하면서 수행 필요 대한 대하여 대해 관련 관한 관하여 관해 내 외부 간 기타 경우 경우에 경우는 경우로 경우가 경우에도 경우에는 기존 전반 전체 일부 항목 요소 내용 부분 방법 방식 수준 정도 다양한 다양한 여럿 같은 동일 최소 최대 이상 이하 제시 제시된 제시하고 제시하여 제시함 확인 확인하여 확인하고 확인함 체크 관리하고 관리하여 관리함 지원하고 지원하여 지원함 제공하고 제공하여 제공함 구축하고 구축하여 구축함 수립하고 수립하여 수립함 운영하고 운영하여 운영함 연계하고 연계하여 연계함 연동하고 연동하여 연동함 위주 구성 구성하고 구성하여 구성함 정의하고 정의하여 정의함 협조 협의 준용 준수 반영 도모 제도 사전 사후 별도 또는 그리고 그러나 하지만 단 다만 본 해당 소요 적극 적극적으로 적극적 최신 최신의 최종 최적 추가 추가로 추가적 병행 병행하여 병행하고 병행함 수시 수시로 상시 상시로 거쳐 중점 핵심 주요 대상 대상으로 대상에 대상자 대상별 필수 필요한 요청 따라 따른 따라서 요구 현황 세부 상세 원활 원활한 원활하게 효율적 효율적인 안정적 안정적인 효율 안정 용이 용이한 용이하도록 근거 근거하여 근거한 근거로 최신성 수신 수신하 송 반드시 원활히 원활하게 적정 형성 바탕 포함 포함하여 포함하고 포함함 사용 사용하여 사용하고 사용함 사용자 사용자가 사용자를 사용자의 사용자별 사용자에 사용자에게 사용자를위해 사용할 사용가능 가능 적용 적용하여 적용하고 적용함 활용 활용하여 활용하고 활용함 확보하여 확보하고 확보함 지원함 지원하며 지원하고 개발하여 개발하고 개발함 구현 구현하여 구현하고 구현함 도입하여 도입하고 도입함 연계시 연계되어 연계되는 연계된 연동되어 연동되는 연동된 연동될 기반 기반으로 기반한 기반하여 효율화 안정화 최적화 고도화 확대 확장 강화 개선 보완 전환 전문화 세분화 공공데이터 관리지침 한국지능정보사회진흥원 행정안전부 고시 주지 않도록 않는 않게 않음 않으며 가능 가능성
""".split())

POST_TOKENS = {
    '체계','방안','계획','절차','기준','전략','대책','시스템','서비스','프로세스','기능','플랫폼','로드맵','지침','가이드','매뉴얼','도구','모듈','환경','체제','조직','프로그램','대응','조치','지표','평가','표준','표준화','센터','포털','데이터','정보','지원','연계','연동','통합','알림','보고','관리','항목','목록','리스트','등록','DB','포맷','양식','자료','수단','방법','모델','모형','분석','진단','감리','대장','통계','메뉴','콘텐츠','정책'
}

GENERIC_TARGETS = {
    '관리','운영','지원','협조','준수','수립','구축','확보','이행','체계','방안','계획','절차','기준','정책','업무','규정','요구사항',
    '사항','내용','현황','데이터','시스템','서비스','플랫폼','프로세스','기능','환경','조직','수단','방법','모델','모형','도구','자료',
    '센터','포털','메뉴','콘텐츠','리스트','목록','체제','전략','대책','지침','가이드','매뉴얼','지표','평가','대응','조치','연계',
    '연동','통합','알림','보고','등록','검색','조회','열람','출력','표준','표준화','감리','대장','진단','분석','가이드라인','대상'
}

COMBINE_PAIRS = {
    ('연계','데이터'): '연계데이터',
    ('상호','운용성'): '상호운용성',
    ('전자정부','프레임워크'): '전자정부프레임워크',
    ('정보','시스템'): '정보시스템',
    ('데이터','품질'): '데이터품질',
    ('데이터','표준화'): '데이터표준화',
    ('품질','관리'): '품질관리',
    ('보안','관리'): '보안관리',
    ('보안','대책'): '보안대책',
    ('보안','정책'): '보안정책',
    ('업무','프로세스'): '업무프로세스',
    ('문서','열람'): '문서열람',
    ('접근','권한'): '접근권한',
    ('개인정보','보호'): '개인정보보호',
    ('개인정보','영향평가'): '개인정보영향평가',
    ('정보','보호'): '정보보호',
    ('정보','공동활용'): '정보공동활용',
    ('연계','정보'): '연계정보',
    ('디지털','감사'): '디지털감사',
    ('전자','감사'): '전자감사',
    ('데이터','관리'): '데이터관리',
    ('데이터','연계'): '데이터연계',
    ('품질','지표'): '품질지표',
    ('성과','지표'): '성과지표',
    ('일정','관리'): '일정관리',
    ('위험','관리'): '위험관리',
    ('감사','자료'): '감사자료',
    ('감사','보고'): '감사보고',
    ('감사','계획'): '감사계획',
    ('감사','결과'): '감사결과',
    ('전체','문서'): '전체문서',
    ('이전','부서'): '이전부서',
    ('부서','생산한'): '부서생산',
    ('생산','문서'): '생산문서',
    ('데이터','타입'): '데이터타입',
    ('테이블','컬럼'): '테이블컬럼',
    ('코드','관리'): '코드관리',
    ('표준','코드'): '표준코드',
    ('보안','취약점'): '보안취약점',
    ('취약점','진단'): '취약점진단',
    ('접근','이력'): '접근이력',
    ('개발','표준'): '개발표준',
    ('기술','지원'): '기술지원',
    ('전산','장비'): '전산장비',
    ('자산','관리'): '자산관리',
}


def normalize(text: str) -> str:
    text = text.replace('\r', ' ').replace('\n', ' ')
    text = re.sub(r'[<>\[\]\(\)_]', ' ', text)
    text = re.sub(r'[○●□■◇◆△▷▶\-\*·•∙‧◦▸▹▻◀◁▪️▶️:,/]', ' ', text)
    return re.sub(r'\s+', ' ', text).strip()


def strip_particle(token: str) -> str:
    tok = token
    for suffix in SUFFIXES:
        if tok.endswith(suffix) and len(tok) > len(suffix) + 1:
            tok = tok[:-len(suffix)]
            break
    if tok.endswith('적인') and len(tok) > 3:
        tok = tok[:-1]
    if tok and tok[-1] in SUFFIX_CHARS and len(tok) > 1:
        tok = tok[:-1]
    return tok


def combine_tokens(tokens: List[str]) -> List[str]:
    combined: List[str] = []
    idx = 0
    while idx < len(tokens):
        if idx + 1 < len(tokens):
            pair = (tokens[idx], tokens[idx + 1])
            if pair in COMBINE_PAIRS:
                combined.append(COMBINE_PAIRS[pair])
                idx += 2
                continue
        combined.append(tokens[idx])
        idx += 1
    return combined


def tokenize_text(text: str) -> List[str]:
    normalized = normalize(text)
    raw_tokens = re.findall(r'[가-힣A-Za-z0-9]+', normalized)
    stripped = [strip_particle(tok) for tok in raw_tokens]
    combined = combine_tokens(stripped)
    ordered: List[str] = []
    seen: set[str] = set()
    for tok in combined:
        if not tok or tok in STOPWORDS or len(tok) < 2:
            continue
        if tok not in seen:
            ordered.append(tok)
            seen.add(tok)
    return ordered


def ensure_descriptive_tokens(tokens: List[str], row: dict) -> List[str]:
    sources = [
        (tokens, 3),
        (tokenize_text(row.get('rfp_title', '')), 2),
        (tokenize_text(row.get('detail_desc', '')), 1),
    ]

    positions: dict[str, Tuple[int, int]] = {}
    candidates: List[tuple[float, int, int, str]] = []
    KEYWORD_BONUS = (
        '데이터','시스템','감사','문서','보고','관리','계획','체계','절차','기준','표준','보안','품질','프로세스','자동','연계',
        '용어','해석','문구','범위','협의','요청서','열람','검색','평가','지표','기능','모듈'
    )
    NEGATIVE_PATTERNS = (
        '관련','의하','의해','의하여','사항','내용','달리','간','등'
    )

    for source_tokens, priority in sources:
        for idx, tok in enumerate(source_tokens):
            if not tok or tok in STOPWORDS or len(tok) < 2:
                continue
            if tok not in positions:
                positions[tok] = (priority, idx)
            score = priority * 20
            score += min(len(tok), 12)
            if tok not in GENERIC_TARGETS:
                score += 18
            else:
                score -= 8
            if any(keyword in tok for keyword in KEYWORD_BONUS):
                score += 6
            if any(pattern in tok for pattern in NEGATIVE_PATTERNS):
                score -= 10
            score -= idx * 0.5
            candidates.append((score, priority, idx, tok))

    candidates.sort(key=lambda item: (-item[0], -item[1], item[2]))

    selected: List[str] = []
    seen: set[str] = set()
    for _, _, _, tok in candidates:
        if tok in seen:
            continue
        selected.append(tok)
        seen.add(tok)
        if len(selected) >= 4:
            break

    if not selected:
        fallback = strip_particle(row.get('rfp_title', '').strip())
        selected = [fallback or '요구사항']

    pruned: List[str] = []
    for tok in selected:
        if any(tok != other and tok in other and len(tok) <= len(other) - 1 for other in selected):
            continue
        pruned.append(tok)
    if pruned:
        selected = pruned

    selected.sort(key=lambda tok: (-positions.get(tok, (0, 0))[0], positions.get(tok, (0, 0))[1]))
    return selected[:4]


def extract_action(detail_desc: str):
    text = normalize(detail_desc)
    tokens = re.findall(r'[가-힣A-Za-z0-9]+', text)
    bases = [strip_particle(tok) for tok in tokens]
    best = None
    for idx, base in enumerate(bases):
        if base not in ACTION_KEYWORDS:
            continue
        context: List[str] = []
        back = idx - 1
        while back >= 0 and len(context) < 6:
            prev = strip_particle(tokens[back])
            if len(prev) >= 2 and prev not in STOPWORDS:
                context.append(prev)
            back -= 1
        context = context[::-1]
        forward_tokens: List[str] = []
        fwd = idx + 1
        while fwd < len(tokens) and len(forward_tokens) < 2:
            nxt = strip_particle(tokens[fwd])
            if nxt in POST_TOKENS and nxt not in STOPWORDS:
                forward_tokens.append(nxt)
                fwd += 1
            else:
                break
        candidate_tokens = [tok for tok in (context + forward_tokens) if tok and tok not in STOPWORDS]
        if not candidate_tokens:
            back = idx - 1
            while back >= 0:
                prev = strip_particle(tokens[back])
                if len(prev) >= 2 and prev not in STOPWORDS:
                    candidate_tokens = [prev]
                    break
                back -= 1
        if not candidate_tokens:
            continue
        candidate_tokens = combine_tokens(candidate_tokens)
        ordered: List[str] = []
        seen: set[str] = set()
        for tok in candidate_tokens:
            if tok in STOPWORDS or len(tok) < 2:
                continue
            if tok not in seen:
                ordered.append(tok)
                seen.add(tok)
        if not ordered:
            continue
        score = sum(len(tok) for tok in ordered)
        if any('데이터' in tok for tok in ordered):
            score += 5
        if any('시스템' in tok for tok in ordered):
            score += 4
        if any('관리' in tok for tok in ordered):
            score += 3
        score += ACTION_BONUS.get(base, 0)
        candidate = {'tokens': ordered[-4:], 'action': base, 'score': score}
        if best is None or candidate['score'] > best['score']:
            best = candidate
    return best


def normalize_focus_token(token: str) -> str:
    token = token.strip()
    if not token:
        return ''
    if ' ' in token:
        parts = [normalize_focus_token(part) for part in token.split()]
        parts = [part for part in parts if part]
        return ' '.join(parts)
    stripped = strip_particle(token)
    if token.endswith('의') and stripped and stripped != token and len(token) <= 3:
        return token
    return stripped


def gather_focus_candidates(row: dict) -> List[str]:
    base_title = row.get('detail_title', '')
    normalized_desc = normalize(row.get('detail_desc', ''))
    raw_tokens = re.findall(r'[가-힣A-Za-z0-9]+', normalized_desc)
    candidates: List[str] = []
    backups: List[str] = []
    seen: set[str] = set()

    def append_candidate(token: str, prefer_primary: bool = True) -> None:
        normalized = normalize_focus_token(token)
        if not normalized:
            return
        if normalized in seen:
            return
        if normalized.isdigit():
            return
        parts = [part for part in normalized.split() if part]
        if not parts:
            return
        if any(part in STOPWORDS or len(part) < 2 for part in parts):
            return
        if normalized in base_title:
            return
        destination = candidates if prefer_primary else backups
        if all(part in GENERIC_TARGETS for part in parts):
            destination = backups
        destination.append(normalized)
        seen.add(normalized)

    for tok in tokenize_text(row.get('detail_desc', '')):
        append_candidate(tok)

    for tok in tokenize_text(row.get('rfp_title', '')):
        append_candidate(tok)

    for tok in raw_tokens:
        if len(tok) < 2:
            continue
        append_candidate(tok)

    for idx in range(len(raw_tokens) - 1):
        combined = f"{raw_tokens[idx]} {raw_tokens[idx + 1]}"
        append_candidate(combined, prefer_primary=False)

    return candidates + backups


def build_title(row: dict, focus_tokens: List[str] | None = None) -> str:
    result = extract_action(row.get('detail_desc', ''))
    if result is None:
        raw_tokens = [strip_particle(tok) for tok in re.findall(r'[가-힣A-Za-z0-9]+', normalize(row.get('rfp_title', '')))]
        base_tokens = [tok for tok in raw_tokens if tok and tok not in STOPWORDS]
        if not base_tokens:
            base_tokens = ['요구사항']
        target_tokens = combine_tokens(base_tokens[:3])
        action = '수립'
        action_label = ACTION_LABELS.get(action, action)
    else:
        action = result['action']
        target_tokens = result['tokens']
        action_label = ACTION_LABELS.get(action, action)
    target_tokens = combine_tokens(target_tokens)
    label_words = set(action_label.split())
    filtered: List[str] = []
    seen: set[str] = set()
    for tok in target_tokens:
        if tok in STOPWORDS or len(tok) < 2:
            continue
        if tok in label_words:
            continue
        if tok not in seen:
            filtered.append(tok)
            seen.add(tok)
    expanded: List[str] = []
    for tok in filtered:
        if tok.endswith('기능') and tok != '기능':
            stem = tok[:-2].strip()
            if stem:
                expanded.append(stem)
            expanded.append('기능')
        else:
            expanded.append(tok)
    filtered = ensure_descriptive_tokens(expanded, row)
    focus_list: List[str] = []
    if focus_tokens:
        for focus in focus_tokens:
            normalized_focus = normalize_focus_token(focus)
            if not normalized_focus or normalized_focus in STOPWORDS:
                continue
            if normalized_focus not in focus_list:
                focus_list.append(normalized_focus)
    if focus_list:
        remainder = [tok for tok in filtered if tok not in focus_list]
        filtered = focus_list + remainder
    filtered = combine_tokens(filtered)
    deduped: List[str] = []
    seen_tokens: set[str] = set()
    for tok in filtered:
        if tok not in seen_tokens:
            deduped.append(tok)
            seen_tokens.add(tok)
    filtered = deduped
    if label_words:
        filtered = [tok for tok in filtered if tok not in label_words]
    redundant_tokens = {'구현'}
    if action_label == '구축':
        redundant_tokens.add('개발')
    elif action_label == '기능 구현':
        redundant_tokens.add('개발')
    elif action_label == '기능 개발':
        redundant_tokens.add('개발')
    pruned_tokens = [tok for tok in filtered if tok not in redundant_tokens]
    if pruned_tokens:
        filtered = pruned_tokens
    target_phrase = ' '.join(filtered[:4])
    title = f"{target_phrase} {action_label}".strip()
    biz = row.get('biz_domain', '').strip()
    if biz:
        title = f"[{biz}] {title}"
    return title


def enforce_unique_titles(rows: List[dict]) -> None:
    while True:
        title_groups: dict[str, List[dict]] = {}
        for row in rows:
            title = row.get('detail_title', '')
            title_groups.setdefault(title, []).append(row)
        duplicates = [group for group in title_groups.values() if len(group) > 1]
        if not duplicates:
            break
        progress = False
        for group in duplicates:
            used_titles: set[str] = set()
            for row in group:
                current_title = row.get('detail_title', '')
                if current_title not in used_titles:
                    used_titles.add(current_title)
                    continue
                candidates = gather_focus_candidates(row)
                updated = False
                for size in range(1, min(3, len(candidates)) + 1):
                    for combo in combinations(candidates, size):
                        new_title = build_title(row, focus_tokens=list(combo))
                        if new_title not in used_titles:
                            row['detail_title'] = new_title
                            used_titles.add(new_title)
                            progress = True
                            updated = True
                            break
                    if updated:
                        break
        if not progress:
            break


def load_rows(path: Path) -> Tuple[List[dict], List[str], str]:
    last_error: Exception | None = None
    for encoding in PREFERRED_ENCODINGS:
        try:
            with path.open('r', encoding=encoding, newline='') as fp:
                reader = csv.DictReader(fp)
                rows = list(reader)
                fieldnames = reader.fieldnames or []
            return rows, fieldnames, encoding
        except UnicodeDecodeError as exc:
            last_error = exc
    if last_error is not None:
        raise last_error
    raise UnicodeDecodeError('unknown', b'', 0, 1, 'No suitable encoding found')


def main():
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"CSV file not found: {CSV_PATH}")

    rows, fieldnames, encoding = load_rows(CSV_PATH)

    if 'detail_title' not in fieldnames:
        fieldnames.append('detail_title')

    for row in rows:
        row['detail_title'] = build_title(row)

    enforce_unique_titles(rows)

    output_rows = [
        {'detail_id': row.get('detail_id', ''), 'detail_title': row.get('detail_title', '')}
        for row in rows
    ]

    with OUTPUT_PATH.open('w', encoding=encoding, newline='') as fp:
        writer = csv.DictWriter(fp, fieldnames=['detail_id', 'detail_title'])
        writer.writeheader()
        writer.writerows(output_rows)


if __name__ == '__main__':
    main()
