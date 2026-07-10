package service

import (
	"context"
	"fmt"

	"github.com/thanhbvha/go-common/examples/graphql_web/graph/model"
)

var (
	users = []*model.User{
		{ID: 1, Name: "Alice"},
		{ID: 2, Name: "Bob"},
		{ID: 3, Name: "Charlie"},
	}

	posts = []*model.Post{
		{ID: 101, Title: "Alice's First Post", AuthorID: 1},
		{ID: 102, Title: "Alice's Second Post", AuthorID: 1},
		{ID: 103, Title: "Bob's Post", AuthorID: 2},
	}
)

func GetUsers() []*model.User {
	return users
}

func GetPosts() []*model.Post {
	return posts
}

func GetPostsByAuthor(authorID int) []*model.Post {
	var res []*model.Post
	for _, p := range posts {
		if p.AuthorID == authorID {
			res = append(res, p)
		}
	}
	return res
}

func FetchUsersBatch(ctx context.Context, ids []int) (map[int]*model.User, error) {
	// Mock DB Query logging
	fmt.Printf("[DB QUERY] SELECT * FROM users WHERE id IN %v\n", ids)

	res := make(map[int]*model.User)
	for _, id := range ids {
		for _, u := range users {
			if u.ID == id {
				res[id] = u
				break
			}
		}
	}
	return res, nil
}
