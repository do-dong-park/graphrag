import datetime
import json
import os
import sys  # 오류 메시지 출력을 위해 추가
from pathlib import Path


def transform_manufacturing_json(input_path: Path, output_dir: Path):
    """
    하나의 원본 제조 공정 JSON 파일을 읽어 GraphRAG 형식으로 변환하고
    지정된 출력 디렉토리에 저장합니다.
    성공 시 True, 실패 시 False를 반환합니다.
    """
    try:
        # 출력 파일 경로 결정
        output_file_path = output_dir / input_path.name

        # 입력 파일 읽기
        with open(input_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        # --- 데이터 추출 (KeyError 방지를 위해 .get() 사용) ---
        main_info = raw_data.get("Main info", {})
        processes = raw_data.get("Processes", [])
        end_items = raw_data.get("Enditems", [])
        caution_flag = raw_data.get("Caution", False)

        # 필수 정보 누락 시 처리
        doc_id = main_info.get("UPG")
        if not doc_id:
            print(
                f"경고: 필수 키 'UPG' 누락으로 파일 처리 건너뜀: {input_path.name}",
                file=sys.stderr,
            )
            return False  # 실패 반환

        assembly_name = main_info.get("조립명", "알 수 없는 작업")  # 제목에도 사용

        # --- 자연어 문장 생성 ---
        lines = []
        model_year = main_info.get("공법차종", "알 수 없는 차종")
        model_name = main_info.get("차종", "알 수 없는 모델")
        line_name = main_info.get("라인", "알 수 없는 라인")
        total_cost = main_info.get("공수합:", 0)  # ':' 포함된 키 사용

        lines.append(
            f"{model_year} {model_name} 모델의 '{assembly_name}' 작업은(는) 라인 '{line_name}'에서 수행됩니다. 총 공수는 {total_cost}입니다."
        )

        if caution_flag:
            lines.append("이 작업에는 주의사항이 있습니다.")

        if processes:
            lines.append(f"작업은 총 {len(processes)}단계로 구성됩니다.")
            for i, p in enumerate(processes, 1):
                action = p.get("동작구분") or "수행"
                desc = p.get("요소작업") or "세부 작업"
                part_location = p.get("작업부위")
                cost = p.get("공수", "0")

                step_text = f"{i}단계: '{desc}' 작업을 {action}합니다"
                if part_location:
                    step_text += f" (작업 부위: {part_location})"
                step_text += f". 이 단계의 공수는 {cost}입니다."
                lines.append(step_text)
        else:
            lines.append("세부 작업 단계 정보가 없습니다.")

        if end_items:
            end_item = end_items[0]  # 첫 번째 항목만 사용 가정
            item_name = end_item.get("품명", "알 수 없는 품목")
            item_number = end_item.get("품번", "알 수 없는 품번")
            lines.append(
                f"최종적으로 '{item_name}' (품번: {item_number}) 부품이 사용됩니다."
            )
        else:
            lines.append("최종 사용 부품 정보가 없습니다.")

        # --- text 필드 조합 ---
        final_text = " ".join([line.strip().removesuffix(".") + "." for line in lines])

        # --- 타임스탬프 처리 ---
        timestamp_str = main_info.get("timestamp")  # 원본에 타임스탬프 키가 있는지 확인
        if not timestamp_str:
            try:
                mtime = os.path.getmtime(input_path)
                timestamp_str = datetime.datetime.fromtimestamp(mtime).isoformat()
            except Exception:
                timestamp_str = datetime.datetime.now().isoformat()

        # --- 최종 변환된 JSON 생성 ---
        converted_data = {
            "id": doc_id,
            "title": assembly_name,
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
    # 입력 및 출력 디렉토리 경로 설정
    input_dir = Path("/home/dongkyu/pdk_ws/study/graphrag/src/data/input/merged_json")
    output_dir = Path(
        "/home/dongkyu/pdk_ws/study/graphrag/src/data/input/assembly_json"
    )

    # 입력 디렉토리 존재 여부 확인
    if not input_dir.is_dir():
        print(f"오류: 입력 디렉토리를 찾을 수 없습니다: {input_dir}", file=sys.stderr)
        sys.exit(1)  # 오류 코드 1로 종료

    # 출력 디렉토리 생성 (없으면)
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        print(
            f"오류: 출력 디렉토리를 생성할 수 없습니다: {output_dir} - {e}",
            file=sys.stderr,
        )
        sys.exit(1)  # 오류 코드 1로 종료

    print(
        f"시작: '{input_dir}' 폴더의 JSON 파일을 변환하여 '{output_dir}'에 저장합니다..."
    )

    total_files = 0
    success_count = 0
    error_count = 0

    # 입력 디렉토리 내 모든 .json 파일 순회
    for json_file in input_dir.glob("*.json"):
        if json_file.is_file():  # 디렉토리가 아닌 파일만 처리
            total_files += 1
            if transform_manufacturing_json(json_file, output_dir):
                success_count += 1
            else:
                error_count += 1

    print("-" * 30)
    print("변환 작업 완료!")
    print(f"총 처리 시도 파일 수: {total_files}")
    print(f"성공적으로 변환된 파일 수: {success_count}")
    print(f"오류 발생 파일 수: {error_count}")
    print(f"결과는 '{output_dir}' 폴더를 확인하세요.")
    print("프로그램을 종료합니다.")
    sys.exit(0)  # 정상 종료
