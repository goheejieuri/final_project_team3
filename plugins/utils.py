# GCS 파일 시스템 등록
def register_gcs(conn):
    import gcsfs
    fs = gcsfs.GCSFileSystem() 
    conn.register_filesystem(fs)

# NLP + 카테고리화
def nlp_modeling(questions):
    import pandas as pd
    from sklearn.metrics.pairwise import cosine_similarity
    import numpy as np
    # 1. 데이터 불러오기
    df = pd.DataFrame({"question": questions})

    # 2. 카테고리 정의 및 임베딩
    categories = {
        "외모": ["예쁜 외모를 가진 사람", "귀여운 스타일의 사람", "패션 센스가 좋은 사람"],    
        "성격": ["성격이 좋은 사람", "자신감 있는 사람", "다정하고 배려심 있는 사람"],
        "능력": ["공부를 잘하는 사람", "집중력이 높은 사람", "문제를 잘 해결하는 사람", "아이큐가 높은 사람"],
        "연애": ["연애를 잘하는 사람", "감정을 잘 표현하는 사람", "스킨십이 자연스러운 사람"],
        "취미": ["요리를 잘하는 사람", "춤이나 노래를 잘하는 사람", "공연을 즐기는 사람"]
    }
    category_texts = [" ".join(words) for words in categories.values()]
    category_names = list(categories.keys())

    from sentence_transformers import SentenceTransformer
    embedding_model = SentenceTransformer("sentence-transformers/paraphrase-multilingual-mpnet-base-v2")
    category_embeddings = embedding_model.encode(category_texts)

    # 3. 질문 임베딩 및 카테고리 할당
    questions = df["question"].tolist()
    question_embeddings = embedding_model.encode(questions, show_progress_bar=True)

    similarities = cosine_similarity(question_embeddings, category_embeddings)
    assigned_categories = [category_names[np.argmax(row)] for row in similarities]
    df["category_nlp"] = assigned_categories

    # 유사도 계산 및 최대 유사도
    df["max_similarity"] = similarities.max(axis=1)  
    for idx, name in enumerate(category_names):
        df[f"sim_{name}"] = similarities[:, idx]

    result_df = df[["question", "category_nlp", "max_similarity"] + [f"sim_{c}" for c in category_names]]
    result_df.loc[result_df["max_similarity"] < 0.35, "category_nlp"] = "기타"
    return result_df


# 질문 데이터 확장
def advanced_questions(question_df, category_df, vote_point_df):
    import numpy as np
    base_df = question_df.merge(
        category_df[["question", "category_nlp"]],
        left_on="question_text", right_on="question", how="left"
    )
    base_df["is_skipped"] = base_df["is_skipped"].fillna(0).astype(int)
    base_df["is_voted"] = base_df["is_voted"].fillna(0).astype(int)

    # 노출/스킵/투표 집계
    skip_exposed_df = base_df.groupby(
        ["question_id", "question_text", "category_nlp"]
    ).agg(
        exposed_impressions=("user_id", "size"),    
        skipped_users=("is_skipped", "sum"),
        voted_users=("is_voted", "sum"),
    ).reset_index()

    # 포인트 집계
    qp_df = base_df.merge(
        vote_point_df,
        on=["question_id", "user_id", "question_piece_id"],
        how="left",
        suffixes=("", "_vote")
    )

    qp_df["delta_points"] = qp_df["delta_points"].apply(
        lambda x: x if isinstance(x, np.ndarray) else []
    )

    def _sum_used(pts):  return sum(p for p in pts if p < 0)
    def _sum_gain(pts):  return sum(p for p in pts if p > 0)
    def _cnt_used(pts):  return sum(1 for p in pts if p < 0)
    def _cnt_gain(pts):  return sum(1 for p in pts if p > 0)
    def _avg_used(pts):
        used = [p for p in pts if p < 0]
        return (sum(used) / len(used)) if used else 0.0

    qp_df["point_used_total"] = qp_df["delta_points"].apply(_sum_used)
    qp_df["point_gain_total"] = qp_df["delta_points"].apply(_sum_gain)
    qp_df["point_used_count"] = qp_df["delta_points"].apply(_cnt_used)
    qp_df["point_gain_count"] = qp_df["delta_points"].apply(_cnt_gain)
    qp_df["point_used_avg"]   = qp_df["delta_points"].apply(_avg_used)

    agg_votes_points = qp_df.groupby(
        ["question_id", "question_text", "category_nlp"]
    ).agg(
        vote_count=("is_voted", "sum"),
        point_used_count=("point_used_count", "sum"),
        point_used_total=("point_used_total", "sum"),
        point_used_avg=("point_used_avg", "mean"),
        point_gain_count=("point_gain_count", "sum"),
        point_gain_total=("point_gain_total", "sum"),
    ).reset_index()

    # 메인 집계 결합
    question_metrics_df = agg_votes_points.merge(
        skip_exposed_df,
        on=["question_id", "question_text", "category_nlp"],
        how="left"
    )

    question_metrics_df["vote_rate"]   = question_metrics_df["voted_users"] / question_metrics_df["exposed_impressions"]
    question_metrics_df["skip_rate"]   = question_metrics_df["skipped_users"] / question_metrics_df["exposed_impressions"]
    question_metrics_df["point_used_rate"] = question_metrics_df["point_used_count"] / question_metrics_df["exposed_impressions"]
    question_metrics_df["avg_votes_per_impression"] = question_metrics_df["voted_users"] / question_metrics_df["exposed_impressions"]

    cols_keep = [
        "question_id","question_text","category_nlp","exposed_impressions",
        "voted_users","skipped_users",
        "vote_count",
        "point_used_count","point_used_total","point_used_avg",
        "point_gain_count","point_gain_total",
        "vote_rate","skip_rate","point_used_rate","avg_votes_per_impression"
    ]
    question_metrics_df = question_metrics_df[cols_keep]
    return question_metrics_df