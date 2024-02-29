{
    "msg_type": "post",
    "content": {
        "post": {
            "en_us": {
                "title": $title,
                "content": [
                    [
                        {
                            "tag": "text",
                            "text": $content
                        }
                    ],
                    [
                        {
                            "tag": "a",
                            "href": $link,
                            "text": "Workflow Details"
                        },
                        {
                            "tag": "text",
                            "text": " | "
                        },
                        {
                            "tag": "a",
                            "href": $page,
                            "text": "Release Notes"
                        }
                    ]
                ]
            }
        }
    }
}
