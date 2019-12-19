package execution

import (
	"testing"
	"time"

	"volcano/common"
)

func TestEqual_Apply(t *testing.T) {
	type args struct {
		variables common.Variables
		left      Expression
		right     Expression
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "simple equal variable check",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeInt(3),
					"b": common.MakeInt(3),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "simple unequal variable check",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeInt(3),
					"b": common.MakeInt(4),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "simple incompatible variable check",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeInt(3),
					"b": common.MakeFloat(3.0),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    false,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rel := &Equal{}
			got, err := rel.Apply(tt.args.variables, tt.args.left, tt.args.right)
			if (err != nil) != tt.wantErr {
				t.Errorf("Equal.Apply() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Equal.Apply() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNotEqual_Apply(t *testing.T) {
	type args struct {
		variables common.Variables
		left      Expression
		right     Expression
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "simple equal variable check",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeInt(3),
					"b": common.MakeInt(3),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "simple unequal variable check",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeInt(3),
					"b": common.MakeInt(4),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "simple incompatible variable check",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeInt(3),
					"b": common.MakeFloat(3.0),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    false,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rel := &NotEqual{}
			got, err := rel.Apply(tt.args.variables, tt.args.left, tt.args.right)
			if (err != nil) != tt.wantErr {
				t.Errorf("NotEqual.Apply() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("NotEqual.Apply() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMoreThan_Apply(t *testing.T) {
	type args struct {
		variables common.Variables
		left      Expression
		right     Expression
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "simple greater than variable check",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeInt(4),
					"b": common.MakeInt(3),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "simple greater than variable check",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeFloat(4.0),
					"b": common.MakeFloat(3.0),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "simple greater than variable check",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeString("b"),
					"b": common.MakeString("a"),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "simple greater than variable check",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeTime(time.Date(2019, 03, 17, 16, 44, 16, 0, time.UTC)),
					"b": common.MakeTime(time.Date(2019, 03, 17, 15, 44, 16, 0, time.UTC)),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "simple incompatible variable check",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeInt(3),
					"b": common.MakeFloat(3.0),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    false,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rel := &MoreThan{}
			got, err := rel.Apply(tt.args.variables, tt.args.left, tt.args.right)
			if (err != nil) != tt.wantErr {
				t.Errorf("MoreThan.Apply() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("MoreThan.Apply() = %v, want %v", got, tt.want)
			}
			gotOpposite, err := rel.Apply(tt.args.variables, tt.args.right, tt.args.left)
			if (err != nil) != tt.wantErr {
				t.Errorf("MoreThan.Apply() opposite error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}
			if gotOpposite != !tt.want {
				t.Errorf("MoreThan.Apply() opposite = %v, want %v", gotOpposite, tt.want)
			}
		})
	}
}

func TestLessThan_Apply(t *testing.T) {
	type args struct {
		variables common.Variables
		left      Expression
		right     Expression
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "simple less than variable check",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeInt(3),
					"b": common.MakeInt(4),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "simple less than variable check",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeFloat(3.0),
					"b": common.MakeFloat(4.0),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "simple less than variable check",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeString("a"),
					"b": common.MakeString("b"),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "simple less than variable check",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeTime(time.Date(2019, 03, 17, 15, 44, 16, 0, time.UTC)),
					"b": common.MakeTime(time.Date(2019, 03, 17, 16, 44, 16, 0, time.UTC)),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "simple incompatible variable check",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeInt(3),
					"b": common.MakeFloat(3.0),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    false,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rel := &LessThan{}
			got, err := rel.Apply(tt.args.variables, tt.args.left, tt.args.right)
			if (err != nil) != tt.wantErr {
				t.Errorf("LessThan.Apply() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("LessThan.Apply() = %v, want %v", got, tt.want)
			}
			gotOpposite, err := rel.Apply(tt.args.variables, tt.args.right, tt.args.left)
			if (err != nil) != tt.wantErr {
				t.Errorf("MoreThan.Apply() opposite error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}
			if gotOpposite != !tt.want {
				t.Errorf("MoreThan.Apply() opposite = %v, want %v", gotOpposite, tt.want)
			}
		})
	}
}

func TestGreaterEqual_Apply(t *testing.T) {
	type args struct {
		variables common.Variables
		left      Expression
		right     Expression
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "simple >= integer variable check (>)",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeInt(5),
					"b": common.MakeInt(4),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},

		{
			name: "simple >= integer variable check (==)",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeInt(4),
					"b": common.MakeInt(4),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},

		{
			name: "simple >= integer variable check (<)",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeInt(4),
					"b": common.MakeInt(6),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    false,
			wantErr: false,
		},

		{
			name: "simple >= string variable check (>)",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeString("baba"),
					"b": common.MakeString("baaa"),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},

		{
			name: "simple >= string variable check (==)",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeString("baba"),
					"b": common.MakeString("baba"),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},

		{
			name: "simple >= string variable check (<)",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeString("baba"),
					"b": common.MakeString("baca"),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    false,
			wantErr: false,
		},

		{
			name: "simple incompatible variables check",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeFloat(3.0),
					"b": common.MakeInt(3),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    false,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rel := &GreaterEqual{}
			got, err := rel.Apply(tt.args.variables, tt.args.left, tt.args.right)
			if (err != nil) != tt.wantErr {
				t.Errorf("LessThan.Apply() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("LessThan.Apply() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLessEqual_Apply(t *testing.T) {
	type args struct {
		variables common.Variables
		left      Expression
		right     Expression
	}
	tests := []struct {
		name    string
		rel     *LessThan
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "simple <= integer variable check (>)",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeInt(5),
					"b": common.MakeInt(4),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    false,
			wantErr: false,
		},

		{
			name: "simple <= integer variable check (==)",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeInt(4),
					"b": common.MakeInt(4),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},

		{
			name: "simple <= integer variable check (<)",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeInt(4),
					"b": common.MakeInt(6),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},

		{
			name: "simple <= string variable check (>)",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeString("baba"),
					"b": common.MakeString("baaa"),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    false,
			wantErr: false,
		},

		{
			name: "simple <= string variable check (==)",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeString("baba"),
					"b": common.MakeString("baba"),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},

		{
			name: "simple <= string variable check (<)",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeString("baba"),
					"b": common.MakeString("baca"),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},

		{
			name: "simple incompatible variables check",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeFloat(3.0),
					"b": common.MakeInt(3),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    false,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rel := &LessEqual{}
			got, err := rel.Apply(tt.args.variables, tt.args.left, tt.args.right)
			if (err != nil) != tt.wantErr {
				t.Errorf("LessThan.Apply() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("LessThan.Apply() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLike_Apply(t *testing.T) {
	type args struct {
		variables common.Variables
		left      Expression
		right     Expression
	}
	tests := []struct {
		name    string
		rel     *Like
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "simple number regex",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeString("123123"),
					"b": common.MakeString("^[0-9]+$"),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "simple number regex",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeString("123123"),
					"b": common.MakeString("^[0-9]$"),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    false,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rel := &Like{}
			got, err := rel.Apply(tt.args.variables, tt.args.left, tt.args.right)
			if (err != nil) != tt.wantErr {
				t.Errorf("Like.Apply() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Like.Apply() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIn_Apply(t *testing.T) {
	type args struct {
		variables common.Variables
		left      Expression
		right     Expression
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "simple in",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeString("123123"),
					"b": common.MakeTuple(
						[]common.Value{
							common.MakeString("123124"),
							common.MakeString("123123"),
						},
					),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "simple in",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeString("123123"),
					"b": common.MakeTuple(
						[]common.Value{
							common.MakeString("123124"),
							common.MakeString("123125"),
						},
					),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "record in",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeTuple([]common.Value{
						common.MakeString("123124"),
						common.MakeInt(13),
					}),
					"b": common.MakeTuple(
						[]common.Value{
							common.MakeTuple([]common.Value{
								common.MakeString("123124"),
								common.MakeInt(13),
							}),
							common.MakeTuple([]common.Value{
								common.MakeString("123123"),
								common.MakeInt(15),
							}),
						},
					),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "record in",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeTuple([]common.Value{
						common.MakeString("123124"),
						common.MakeInt(13),
					}),
					"b": common.MakeTuple(
						[]common.Value{
							common.MakeTuple([]common.Value{
								common.MakeString("123125"),
								common.MakeInt(13),
							}),
							common.MakeTuple([]common.Value{
								common.MakeString("123123"),
								common.MakeInt(15),
							}),
						},
					),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "simple in",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeString("123123"),
					"b": common.MakeTuple(
						[]common.Value{
							common.MakeString("123123"),
							common.MakeInt(13),
						},
					),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "simple in",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeString("123123"),
					"b": common.MakeString("123123"),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rel := &In{}
			got, err := rel.Apply(tt.args.variables, tt.args.left, tt.args.right)
			if (err != nil) != tt.wantErr {
				t.Errorf("In.Apply() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("In.Apply() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNotIn_Apply(t *testing.T) {
	type args struct {
		variables common.Variables
		left      Expression
		right     Expression
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "simple in",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeString("123123"),
					"b": common.MakeTuple(
						[]common.Value{
							common.MakeString("123124"),
							common.MakeString("123123"),
						},
					),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "simple in",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeString("123123"),
					"b": common.MakeTuple(
						[]common.Value{
							common.MakeString("123124"),
							common.MakeString("123125"),
						},
					),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "record in",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeTuple([]common.Value{
						common.MakeString("123124"),
						common.MakeInt(13),
					}),
					"b": common.MakeTuple(
						[]common.Value{
							common.MakeTuple([]common.Value{
								common.MakeString("123124"),
								common.MakeInt(13),
							}),
							common.MakeTuple([]common.Value{
								common.MakeString("123123"),
								common.MakeInt(15),
							}),
						},
					),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "record in",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeTuple([]common.Value{
						common.MakeString("123124"),
						common.MakeInt(13),
					}),
					"b": common.MakeTuple(
						[]common.Value{
							common.MakeTuple([]common.Value{
								common.MakeString("123125"),
								common.MakeInt(13),
							}),
							common.MakeTuple([]common.Value{
								common.MakeString("123123"),
								common.MakeInt(15),
							}),
						},
					),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "simple in",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeString("123123"),
					"b": common.MakeTuple(
						[]common.Value{
							common.MakeString("123123"),
							common.MakeInt(13),
						},
					),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    false,
			wantErr: false,
		},
		{
			name: "simple in",
			args: args{
				variables: map[common.VariableName]common.Value{
					"a": common.MakeString("123123"),
					"b": common.MakeString("123123"),
				},
				left:  NewVariable("a"),
				right: NewVariable("b"),
			},
			want:    false,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rel := &NotIn{}
			got, err := rel.Apply(tt.args.variables, tt.args.left, tt.args.right)
			if (err != nil) != tt.wantErr {
				t.Errorf("In.Apply() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("In.Apply() = %v, want %v", got, tt.want)
			}
		})
	}
}
