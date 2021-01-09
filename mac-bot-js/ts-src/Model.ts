export type Movie = {
  _id: string;
  rank: number;
  title: string;
  genre: string;
  description: string;
  director: string;
  actors: string;
  year: number;
  runtime: number;
  rating: number;
  votes: number;
  revenue: number;
  metascore: number;
}


export type Actor = {
  id: number;
  name: string;
}

export type Genre = {
  id: number;
  name: string;
}

export type Comment = {
  id: number;
  text: string;
  at: Date;
}

export type Added = {
  at: Date;
}

export type Requested = {
  at: Date;
}

export type Liked = {
  at: Date;
  rank: number;
}

export const likedValues = [1,2,3,4,5];

export type User = {
  username?: string;
  last_name?: string;
  first_name?: string;
  id: number;
  is_bot: boolean;
  language_code?: string;
}