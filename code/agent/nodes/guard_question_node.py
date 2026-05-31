from services.security_service import detect_question_risk


def guard_question_node(state):
    question = (state.get("user_question") or "").strip()

    if not question:
        return {
            "error": "Câu hỏi không được để trống.",
            "safety": {
                "allowed": False,
                "reason": "Câu hỏi rỗng.",
                "category": "empty_question",
            },
        }

    safety = detect_question_risk(question)
    if not safety["allowed"]:
        return {
            "error": (
                "Yêu cầu bị từ chối vì hệ thống chỉ hỗ trợ phân tích dữ liệu "
                "ở chế độ chỉ đọc. Vui lòng đặt câu hỏi phân tích không yêu "
                "cầu thay đổi dữ liệu hoặc bỏ qua quy trình truy vấn."
            ),
            "safety": safety,
        }

    return {
        "safety": safety,
        "error": None,
    }
