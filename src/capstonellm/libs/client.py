import time

import requests


class StackOverflowClient:
    BASE_URL = "https://api.stackexchange.com/2.3"

    def __init__(self, site="stackoverflow"):
        self.site = site
        self.session = requests.Session()

    def _get_all_pages(self, endpoint: str, params: dict) -> list[dict]:
        items = []
        page = 1

        while True:
            response = self.session.get(
                f"{self.BASE_URL}/{endpoint}",
                params={
                    **params,
                    "page": page,
                    "pagesize": 100,
                },
            )
            response.raise_for_status()

            data = response.json()

            if "error_id" in data:
                raise RuntimeError(
                    f"Stack Exchange API error: "
                    f"{data['error_name']} - "
                    f"{data['error_message']}"
                )

            page_items = data.get("items", [])
            items.extend(page_items)

            print(
                f"{endpoint} - page {page}: "
                f"{len(page_items)} items, "
                f"quota remaining: "
                f"{data.get('quota_remaining')}"
            )

            if "backoff" in data:
                backoff = data["backoff"]

                print(
                    f"API backoff requested: "
                    f"waiting {backoff} seconds"
                )

                time.sleep(backoff)

            if not data.get("has_more", False):
                break

            page += 1

        return items

    def get_questions(self, tag: str) -> list[dict]:
        return self._get_all_pages(
            "questions",
            {
                "site": self.site,
                "tagged": tag,
                "filter": "withbody",
            },
        )

    def get_answers(self, question_id: int) -> list[dict]:
        return self._get_all_pages(
            f"questions/{question_id}/answers",
            {
                "site": self.site,
                "filter": "withbody",
            },
        )