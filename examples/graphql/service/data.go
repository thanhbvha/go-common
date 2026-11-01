package service

import (
	"context"

	"github.com/thanhbvha/go-common/examples/graphql/graph/model"
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
	if err := ctx.Err(); err != nil {
		return nil, err
	}

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

func FetchPostsByAuthorBatch(ctx context.Context, ids []int) (map[int][]*model.Post, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result := make(map[int][]*model.Post, len(ids))
	for _, id := range ids {
		result[id] = []*model.Post{}
	}
	for _, post := range posts {
		if _, ok := result[post.AuthorID]; ok {
			result[post.AuthorID] = append(result[post.AuthorID], post)
		}
	}
	return result, nil
}
