import datetime
import json
import os
import sys
from pathlib import Path


def transform_parts_list_json(input_path: Path, output_dir: Path):
    """
    하나의 부품 목록 JSON 파일을 읽어 GraphRAG 형식으로 변환하고
    지정된 출력 디렉토리에 저장합니다.
    성공 시 True, 실패 시 False를 반환합니다.
    """
    try:
        # 출력 파일 경로 결정
        output_file_path = output_dir / input_path.name

        # 입력 파일 읽기
        with open(input_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        # --- 데이터 추출 ---
        main_upg = raw_data.get("upg")
        if not main_upg:
            print(
                f"경고: 필수 키 'upg' 누락으로 파일 처리 건너뜀: {input_path.name}",
                file=sys.stderr,
            )
            return False  # 실패 반환

        parts = raw_data.get("part_list", [])

        # --- 자연어 문장 생성 ---
        lines = []
        # 제목 생성 (예: "CVUPGBA813AE 부품 목록")
        title = f"{main_upg} 부품 목록"

        lines.append(f"유닛(UPG) {main_upg}은(는) 다음 부품들로 구성됩니다:")

        if parts:
            for i, part in enumerate(parts, 1):
                part_no = part.get("part_no", "N/A")
                part_name = part.get("part_name", "N/A")
                quantity = part.get("quantity", 0)
                # 들여쓰기와 함께 각 부품 정보 추가
                lines.append(
                    f"  {i}. 부품 번호: {part_no}, 부품명: {part_name}, 수량: {quantity}"
                )
        else:
            lines.append("  구성 부품 정보가 없습니다.")

        # 줄바꿈(\n)으로 각 줄을 연결하여 가독성 높임
        final_text = "\n".join(lines)

        # --- 타임스탬프 처리 (원본에 없으므로 현재 시간 사용) ---
        timestamp_str = datetime.datetime.now().isoformat()

        # --- 최종 변환된 JSON 생성 ---
        converted_data = {
            "id": main_upg,
            "title": title,
            "text": final_text,
            "timestamp": timestamp_str,
        }

        # --- 결과 파일 저장 ---
        with open(output_file_path, "w", encoding="utf-8") as f:
            json.dump(converted_data, f, ensure_ascii=False, indent=2)

        return True  # 성공 반환

    except json.JSONDecodeError:
        print(f"오류: JSON 파싱 실패 - {input_path.name}", file=sys.stderr)
        return False  # 실패 반환
    except Exception as e:
        print(f"오류: 처리 중 예외 발생 ({input_path.name}) - {e}", file=sys.stderr)
        return False  # 실패 반환


# --- 스크립트 실행 부분 ---
if __name__ == "__main__":
    # 중요: 이 경로는 *부품 목록 JSON 파일만 들어있는 폴더*여야 합니다!
    input_dir = Path(
        "/home/dongkyu/pdk_ws/study/graphrag/src/data/input/raw_data/raw_bom_json"
    )  # <--- 부품 목록 JSON이 있는 실제 경로로 수정하세요!

    # 출력 경로는 이전과 동일하게 사용하거나, 필요시 수정하세요.
    output_dir = Path(
        "/home/dongkyu/pdk_ws/study/graphrag/src/data/input/bom_json"
    )  # <--- 변환된 부품 목록 JSON을 저장할 경로

    # 입력 디렉토리 존재 여부 확인
    if not input_dir.is_dir():
        print(f"오류: 입력 디렉토리를 찾을 수 없습니다: {input_dir}", file=sys.stderr)
        sys.exit(1)

    # 출력 디렉토리 생성 (없으면)
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        print(
            f"오류: 출력 디렉토리를 생성할 수 없습니다: {output_dir} - {e}",
            file=sys.stderr,
        )
        sys.exit(1)

    print(
        f"시작: '{input_dir}' 폴더의 부품 목록 JSON 파일을 변환하여 '{output_dir}'에 저장합니다..."
    )

    total_files = 0
    success_count = 0
    error_count = 0

    # 입력 디렉토리 내 모든 .json 파일 순회
    for json_file in input_dir.glob("*.json"):
        if json_file.is_file():
            total_files += 1
            # 부품 목록 변환 함수 호출
            if transform_parts_list_json(json_file, output_dir):
                success_count += 1
            else:
                error_count += 1

    print("-" * 30)
    print("변환 작업 완료!")
    print(f"총 처리 시도 파일 수: {total_files}")
    print(f"성공적으로 변환된 파일 수: {success_count}")
    print(f"오류 발생 파일 수: {error_count}")
    print(f"결과는 '{output_dir}' 폴더를 확인하세요.")
