# Snippet #1 version 1.0 - GlobalSearch 기반 전역 검색 실행기

import asyncio
import os
from pathlib import Path

import pandas as pd
import tiktoken

from graphrag.api import build_index
from graphrag.config.enums import ModelType
from graphrag.config.load_config import load_config
from graphrag.config.models.language_model_config import LanguageModelConfig
from graphrag.index.typing.pipeline_run_result import PipelineRunResult
from graphrag.language_model.manager import ModelManager
from graphrag.query.indexer_adapters import (
    read_indexer_communities,
    read_indexer_entities,
    read_indexer_reports,
)
from graphrag.query.structured_search.global_search.community_context import (
    GlobalCommunityContext,
)
from graphrag.query.structured_search.global_search.search import GlobalSearch

PROJECT_DIRECTORY = Path.cwd().resolve() / "src"
GRAPHRAG_CONFIG = load_config(PROJECT_DIRECTORY)


async def run_index():
    print("📦 인덱스 빌드 시작...")
    try:
        results: list[PipelineRunResult] = await build_index(config=GRAPHRAG_CONFIG)
        for r in results:
            print(
                f"✅ 워크플로우 {r.workflow} - 상태: {'성공' if not r.errors else f'오류: {r.errors}'}"
            )
    except Exception as e:
        print(f"❌ 인덱스 빌드 실패: {e}")
        raise


async def main():
    await run_index()
    # LLM 설정
    print("🔧 LLM 초기화...")
    llm_config = GRAPHRAG_CONFIG.get_language_model_config("default_chat_model")

    model = ModelManager().get_or_create_chat_model(
        name="global_search",
        model_type=llm_config.type,
        config=llm_config,
    )

    # 토크나이저 설정
    try:
        token_encoder = tiktoken.encoding_for_model(llm_config.model)
    except Exception:
        token_encoder = tiktoken.get_encoding("cl100k_base")

    # 데이터 로드
    print("📂 결과 데이터 로딩 중...")
    output_dir = PROJECT_DIRECTORY / "data" / "output"
    level = 2

    try:
        community_df = pd.read_parquet(output_dir / "communities.parquet")
        entity_df = pd.read_parquet(output_dir / "entities.parquet")
        report_df = pd.read_parquet(output_dir / "community_reports.parquet")

        communities = read_indexer_communities(community_df, report_df)
        reports = read_indexer_reports(report_df, community_df, level)
        entities = read_indexer_entities(entity_df, community_df, level)

    except FileNotFoundError as e:
        print(f"❌ Parquet 파일 누락: {e}")
        return

    # Context Builder 설정
    context_builder = GlobalCommunityContext(
        community_reports=reports,
        communities=communities,
        entities=entities,
        token_encoder=token_encoder,
    )

    # GlobalSearch 파라미터 정의
    search_engine = GlobalSearch(
        model=model,
        context_builder=context_builder,
        token_encoder=token_encoder,
        max_data_tokens=12000,
        map_llm_params={
            "max_tokens": 1000,
            "temperature": 0.0,
            "response_format": {"type": "json_object"},
        },
        reduce_llm_params={"max_tokens": 2000, "temperature": 0.0},
        context_builder_params={
            "use_community_summary": False,
            "shuffle_data": True,
            "include_community_rank": True,
            "min_community_rank": 0,
            "community_rank_name": "rank",
            "include_community_weight": True,
            "community_weight_name": "occurrence weight",
            "normalize_community_weight": True,
            "max_tokens": 12000,
            "context_name": "Reports",
        },
        allow_general_knowledge=True,
        json_mode=True,
        concurrent_coroutines=32,
        response_type="multiple paragraphs",
    )

    # 쿼리 실행
    query = "프론트 도어 체커 커버 앗세이 장착(RH)법을 생성해줘."
    print(f"🔍 전역 검색 쿼리 실행 중: {query}")
    try:
        result = await search_engine.search(query=query)
        print("\n📄 결과:")
        print(result.response)

        print("\n📊 통계:")
        print(f"LLM 호출 수: {result.llm_calls}")
        print(f"프롬프트 토큰: {result.prompt_tokens}")
        print(f"출력 토큰: {result.output_tokens}")
    except Exception as e:
        print(f"❌ 검색 중 오류: {e}")


if __name__ == "__main__":
    asyncio.run(main())
